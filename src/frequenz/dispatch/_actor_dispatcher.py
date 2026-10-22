# License: All rights reserved
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Helper class to manage actors based on dispatches."""

import asyncio
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Protocol, runtime_checkable
from warnings import warn

from frequenz.channels import Broadcast, Receiver, Sender, select
from frequenz.channels.timer import SkipMissedAndDrift, Timer
from frequenz.client.dispatch.types import DispatchId, TargetComponents
from frequenz.core.id import BaseId
from frequenz.sdk.actor import Actor, BackgroundService
from typing_extensions import deprecated

from ._dispatch import Dispatch

_logger = logging.getLogger(__name__)


@runtime_checkable
class ContinuationCapable(Protocol):
    """An actor that can explicitly accept or refuse a continuation update.

    `ActorDispatcher` only ever reuses an existing actor for an unrelated
    dispatch (see `continuation_predicate`) when that actor's CLASS implements
    this protocol -- checked with `isinstance()` BEFORE anything is sent and
    BEFORE the predecessor's actor is left running under the assumption it
    will pick the update up. An actor that does not implement this (e.g. an
    existing actor written before this contract existed) is never silently
    left stale: `ActorDispatcher` stops it and starts a fresh actor for the
    new dispatch instead, exactly as if no continuation had been attempted.
    """

    async def accept_continuation(self, update: "DispatchInfo") -> bool:
        """Attempt to accept a continuation update.

        If this returns `True`, the actor MUST have adopted `update` as its
        new current dispatch and submitted the payload's new proposal through
        its existing proposal pool before returning.  This is proof of actor
        state adoption and proposal submission, not a promise that physical
        power has already changed or that downstream aggregation has already
        completed synchronously.

        Args:
            update: The continuation update, with `update.continuation_of`
                equal to the dispatch id this actor is currently running
                under, and `update._src` the new dispatch to adopt.

        Returns:
            `True` if the new dispatch state was adopted and its proposal was
                submitted; `False` to refuse, in which case the caller treats
                this exactly as if the actor were not continuation-capable at
                all (it will be stopped, not left running with a stale
                payload).
        """


class DispatchActorId(BaseId, str_prefix="DA"):
    """An ID for a dispatch actor."""

    def __init__(self, dispatch_id: DispatchId | int) -> None:
        """Initialize the DispatchActorId.

        Args:
            dispatch_id: The ID of the dispatch this actor is associated with.
        """
        super().__init__(int(dispatch_id))


@dataclass(frozen=True, kw_only=True)
class DispatchInfo:
    """An event emitted when the dispatch changes."""

    @property
    @deprecated("'components' is deprecated, use 'target' instead.")
    def components(self) -> TargetComponents:
        """The target components.

        Warning: Deprecated in v0.10.3
            Use [`target`][..target] instead.
        """
        return self.target

    target: TargetComponents
    """Target components to be used."""

    dry_run: bool
    """Whether this is a dry run."""

    options: dict[str, Any]
    """Additional options."""

    _src: Dispatch
    """The dispatch that triggered this update."""

    continuation_of: DispatchId | None
    """If set, this update continues a PREVIOUS dispatch with this id.

    Only ever set by `ActorDispatcher` itself, when it has decided (via an
    explicit, opt-in `continuation_predicate`) that `_src` is an exact,
    compatible continuation of a dispatch the receiving actor was already
    running under. A continuation-AWARE actor MAY accept this update and adopt
    `_src` as its new dispatch -- after validating that `continuation_of`
    matches the dispatch id it is currently running under -- even though
    `_src.id` differs from that id.

    An actor that does not look at this field is unaffected: it defaults to
    `None`, and an actor that only ever checks `_src.id` against its own
    dispatch id (as every actor does today) keeps rejecting cross-id updates
    exactly as before. This field cannot by itself weaken any existing guard;
    an actor has to explicitly opt in to honouring it.
    """

    def __init__(
        self,
        *,
        target: TargetComponents | None = None,
        components: TargetComponents | None = None,
        dry_run: bool,
        options: dict[str, Any],
        _src: Dispatch,
        continuation_of: DispatchId | None = None,
    ) -> None:
        """Initialize the DispatchInfo.

        Args:
            target: Target components to be used.
            components: Deprecated alias for `target`.
            dry_run: Whether this is a dry run.
            options: Additional options.
            _src: The dispatch that triggered this update.
            continuation_of: If set, the id of a previous dispatch this update
                continues. See the `continuation_of` attribute for details.

        Raises:
            ValueError: If both `target` and `components` are set, or if neither is set.
        """
        if target is not None and components is not None:
            raise ValueError("Only one of 'target' or 'components' can be set.")

        # Use components if target is not provided (backwards compatibility)
        if target is None:
            if components is None:
                raise ValueError("One of 'target' or 'components' must be set.")
            target = components
            warn(
                "'components' is deprecated, use 'target' instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        object.__setattr__(self, "target", target)
        object.__setattr__(self, "dry_run", dry_run)
        object.__setattr__(self, "options", options)
        object.__setattr__(self, "_src", _src)
        object.__setattr__(self, "continuation_of", continuation_of)


class ActorDispatcher(
    BackgroundService
):  # pylint: disable=too-many-instance-attributes
    """A helper class to manage actors based on dispatches.

    Example:
        ```python
        import os
        import asyncio
        from typing import Any, Self
        from typing import override
        from frequenz.dispatch import Dispatcher, ActorDispatcher, DispatchInfo
        from frequenz.client.common.microgrid.components import ComponentCategory
        from frequenz.channels import Receiver, Broadcast, select, selected_from
        from frequenz.sdk.actor import Actor, run

        class MyActor(Actor):
            def __init__(
                    self,
                    *,
                    name: str | None = None,
            ) -> None:
                super().__init__(name=name)
                self._dispatch_updates_receiver: Receiver[DispatchInfo] | None = None
                self._dry_run: bool = False
                self._options: dict[str, Any] = {}

            @classmethod
            def new_with_dispatch(
                    cls,
                    initial_dispatch: DispatchInfo,
                    dispatch_updates_receiver: Receiver[DispatchInfo],
                    *,
                    name: str | None = None,
            ) -> Self:
                self = cls(name=name)
                self._dispatch_updates_receiver = dispatch_updates_receiver
                self._update_dispatch_information(initial_dispatch)
                return self

            @override
            async def _run(self) -> None:
                other_recv = Broadcast[Any](name="other").new_receiver()

                if self._dispatch_updates_receiver is None:
                    async for msg in other_recv:
                        # do stuff
                        ...
                else:
                    await self._run_with_dispatch(other_recv)

            async def _run_with_dispatch(self, other_recv: Receiver[Any]) -> None:
                async for selected in select(self._dispatch_updates_receiver, other_recv):
                    if selected_from(selected, self._dispatch_updates_receiver):
                        self._update_dispatch_information(selected.message)
                    elif selected_from(selected, other_recv):
                        # do stuff
                        ...
                    else:
                        assert False, f"Unexpected selected receiver: {selected}"

            def _update_dispatch_information(self, dispatch_update: DispatchInfo) -> None:
                print("Received update:", dispatch_update)
                self._dry_run = dispatch_update.dry_run
                self._options = dispatch_update.options
                match dispatch_update.target:
                    case []:
                        print("Dispatch: Using all components")
                    case list() as ids if isinstance(ids[0], int):
                        component_ids = ids
                    case [ComponentCategory.BATTERY, *_]:
                        component_category = ComponentCategory.BATTERY
                    case unsupported:
                        print(
                            "Dispatch: Requested an unsupported selector %r, "
                            "but only component IDs or category BATTERY are supported.",
                            unsupported,
                        )

        async def main():
            url = os.getenv("DISPATCH_API_URL", "grpc://dispatch.url.goes.here.example.com")
            auth_key = os.getenv("DISPATCH_API_AUTH_KEY", "some-key")
            sign_secret = os.getenv("DISPATCH_API_SIGN_SECRET")

            microgrid_id = 1

            async with Dispatcher(
                microgrid_id=microgrid_id,
                server_url=url,
                auth_key=auth_key,
                sign_secret=sign_secret,
            ) as dispatcher:
                status_receiver = await dispatcher.new_running_state_event_receiver("EXAMPLE_TYPE")

                managing_actor = ActorDispatcher(
                    actor_factory=MyActor.new_with_dispatch,
                    running_status_receiver=status_receiver,
                )

                await run(managing_actor)
        ```
    """

    @dataclass(kw_only=True)
    class ActorAndChannel:
        """An actor and its dispatch update sender."""

        actor: Actor
        """The actor."""

        channel: Broadcast[DispatchInfo]
        """The channel for dispatch updates."""

        sender: Sender[DispatchInfo]
        """The sender for dispatch updates."""

        current_dispatch: Dispatch
        """The most recent dispatch this actor is known to be running under.

        Updated on every ordinary same-identity update AND on every successful
        continuation, so that `continuation_predicate` always compares a new
        dispatch against the FRESHEST known state of a running actor (this is
        what makes multi-step continuation chains work: a third dispatch is
        compared against the second, not the first).
        """

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        actor_factory: Callable[
            [DispatchInfo, Receiver[DispatchInfo]], Awaitable[Actor]
        ],
        running_status_receiver: Receiver[Dispatch],
        dispatch_identity: Callable[[Dispatch], DispatchActorId] | None = None,
        retry_interval: timedelta = timedelta(seconds=60),
        continuation_predicate: Callable[[Dispatch, Dispatch], bool] | None = None,
        dispatches_snapshot: Callable[[], Mapping[DispatchId, Dispatch]] | None = None,
        continuation_accept_timeout: timedelta = timedelta(seconds=5),
    ) -> None:
        """Initialize the dispatch handler.

        Args:
            actor_factory: A callable that creates an actor with some initial dispatch
                information.
            running_status_receiver: The receiver for dispatch running status changes.
                EVERY event (including every stop) is always processed here;
                nothing is ever filtered out upstream on this class's behalf --
                see `continuation_predicate`/`dispatches_snapshot` for why.
            dispatch_identity: A function to identify to which actor a dispatch refers.
                By default, it uses the dispatch ID.
            retry_interval: How long to wait until trying to start failed actors again.
            continuation_predicate: Opt-in, default `None` (no behaviour change).
                Called as `continuation_predicate(current_dispatch, new_dispatch)`.
                Used in two symmetric places.  Both require a UNIQUE PAIR:
                the candidate predecessor must match exactly one successor,
                and that successor must match exactly one predecessor.  A
                one-sided unique match is insufficient.
                * On a START event for a dispatch that does NOT already match an
                  existing actor by `dispatch_identity`: if it returns `True` for
                   EXACTLY ONE currently-running actor, and that actor's
                   dispatch has EXACTLY ONE matching successor in
                   `dispatches_snapshot`, that dispatch's start is
                  routed to the EXISTING actor as a continuation (see
                  `DispatchInfo.continuation_of`) instead of creating a second,
                  independent actor ("start-first" ordering).
                * On a STOP event for a dispatch with a running actor: if
                   `dispatches_snapshot` finds EXACTLY ONE other,
                   already-started, not-yet-registered dispatch that
                   `continuation_predicate` says this one continues INTO, and
                   that successor has EXACTLY ONE matching predecessor in the
                   same snapshot, the stop is resolved as a continuation into
                   that dispatch instead ("stop-first" ordering) -- see
                   `dispatches_snapshot`.
                In both places, zero or more than one match means no
                continuation is attempted: an ambiguous match is never guessed
                at, and a dispatch with no known successor/predecessor is
                handled exactly as it always was.
            dispatches_snapshot: Opt-in, default `None`. A zero-argument
                callable returning a live, READ-ONLY view of every dispatch
                currently known to the event source (e.g.
                `DispatchScheduler.dispatches_snapshot`) -- used ONLY for the
                pair-uniqueness checks above. This class never
                mutates anything through it, and never uses it to delay or
                suppress a stop: by the time `_stop_actor` is called, the stop
                is already being acted on; this is purely input to deciding
                WHAT to do about it, synchronously, in the same call. Required
                (alongside `continuation_predicate`) for either ordering to be
                resolved.  Without it, continuation fails closed because the
                opposite side of the pair cannot be proven unique.
            continuation_accept_timeout: An upper bound on how long a
                candidate actor's `accept_continuation` may run before it is
                treated as a refusal (same fallback as a `False` return or a
                raised exception: the predecessor is stopped and a fresh actor
                is started instead). Bounds how long one hanging or
                badly-behaved actor can stall this whole service's `_run()`
                loop -- every other dispatch is queued behind it until this
                resolves one way or another.
        """
        super().__init__()
        self._dispatch_identity: Callable[[Dispatch], DispatchActorId] = (
            dispatch_identity if dispatch_identity else lambda d: DispatchActorId(d.id)
        )
        self._continuation_predicate = continuation_predicate
        self._dispatches_snapshot = dispatches_snapshot
        self._continuation_accept_timeout = continuation_accept_timeout

        self._dispatch_rx = running_status_receiver
        self._retry_timer_rx = Timer(retry_interval, SkipMissedAndDrift())
        self._actor_factory = actor_factory
        self._actors: dict[DispatchActorId, ActorDispatcher.ActorAndChannel] = {}
        self._failed_dispatches: dict[DispatchActorId, Dispatch] = {}
        """Failed dispatches that will be retried later."""

    def start(self) -> None:
        """Start the background service."""
        self._tasks.add(asyncio.create_task(self._run()))

    def _find_unique_continuation_candidate(
        self, dispatch: Dispatch
    ) -> tuple[DispatchActorId, "ActorDispatcher.ActorAndChannel"] | None:
        """Find the single running actor `dispatch` exactly continues, if any.

        Args:
            dispatch: The newly-arrived dispatch, not already matched by
                `dispatch_identity` to an existing actor.

        Returns:
            The `(identity, actor_and_channel)` of the unique continuation
            pair, or `None` if either side of the pair is not unique.
        """
        if self._continuation_predicate is None or self._dispatches_snapshot is None:
            return None

        candidates = [
            (identity, actor_and_channel)
            for identity, actor_and_channel in self._actors.items()
            if self._continuation_predicate(
                actor_and_channel.current_dispatch, dispatch
            )
        ]

        if len(candidates) != 1:
            if len(candidates) > 1:
                _logger.warning(
                    "Dispatch %s matches %d continuation predecessor candidates "
                    "(%s); refusing to guess, starting an independent actor instead",
                    dispatch.id,
                    len(candidates),
                    [identity for identity, _ in candidates],
                )
            return None

        predecessor_identity, predecessor = candidates[0]
        snapshot = self._dispatches_snapshot()
        predecessor_candidates = self._matching_predecessors(dispatch, snapshot)
        if (
            len(predecessor_candidates) != 1
            or predecessor_candidates[0].id != predecessor.current_dispatch.id
        ):
            _logger.warning(
                "Dispatch %s matches %d snapshot predecessor candidates (%s); "
                "refusing a one-sided actor match and starting an independent "
                "actor instead",
                dispatch.id,
                len(predecessor_candidates),
                [candidate.id for candidate in predecessor_candidates],
            )
            return None

        successor_candidates = self._matching_successors(
            predecessor.current_dispatch, snapshot
        )
        if len(successor_candidates) != 1 or successor_candidates[0].id != dispatch.id:
            _logger.warning(
                "Dispatch %s's predecessor %s matches %d successor candidates "
                "(%s); refusing a one-sided match and starting an independent "
                "actor instead",
                dispatch.id,
                predecessor.current_dispatch.id,
                len(successor_candidates),
                [candidate.id for candidate in successor_candidates],
            )
            return None

        return predecessor_identity, predecessor

    def _matching_successors(
        self, predecessor: Dispatch, snapshot: Mapping[DispatchId, Dispatch]
    ) -> list[Dispatch]:
        """Return all already-started snapshot successors of `predecessor`."""
        assert self._continuation_predicate is not None
        now = datetime.now(timezone.utc)
        return [
            other
            for other in snapshot.values()
            if other.id != predecessor.id
            and other.started_at(now)
            and self._continuation_predicate(predecessor, other)
        ]

    def _matching_predecessors(
        self, successor: Dispatch, snapshot: Mapping[DispatchId, Dispatch]
    ) -> list[Dispatch]:
        """Return all snapshot predecessors exactly continued by `successor`."""
        assert self._continuation_predicate is not None
        return [
            other
            for other in snapshot.values()
            if other.id != successor.id
            and self._continuation_predicate(other, successor)
        ]

    def _find_unique_continuation_successor(
        self, predecessor: Dispatch
    ) -> Dispatch | None:
        """Find the unique started successor of `predecessor`, if any.

        This uses the live `dispatches_snapshot`. Called when `predecessor`'s
        own stop is about to be processed, so this
        can resolve "stop-first" ordering WITHOUT waiting for the successor's
        own start event to arrive later -- when it does, it simply hits the
        ordinary same-identity "already running" update path, because the
        actor has, by then, already been re-keyed onto it.

        Args:
            predecessor: The dispatch whose stop is being processed.

        Returns:
            The unique successor dispatch, or `None` if there is no
                `continuation_predicate`/`dispatches_snapshot` configured, or
                zero, or more than one, matching candidate.
        """
        if self._continuation_predicate is None or self._dispatches_snapshot is None:
            return None

        snapshot = self._dispatches_snapshot()
        candidates = self._matching_successors(predecessor, snapshot)

        if len(candidates) != 1:
            if len(candidates) > 1:
                _logger.warning(
                    "Dispatch %s has %d ambiguous exact-continuation successor "
                    "candidates (%s) in the known-dispatches snapshot; refusing "
                    "to guess, stopping normally instead",
                    predecessor.id,
                    len(candidates),
                    [candidate.id for candidate in candidates],
                )
            return None

        successor = candidates[0]
        predecessor_candidates = self._matching_predecessors(successor, snapshot)
        if (
            len(predecessor_candidates) != 1
            or predecessor_candidates[0].id != predecessor.id
        ):
            _logger.warning(
                "Dispatch %s's successor %s matches %d predecessor candidates "
                "(%s); refusing a one-sided match and stopping normally instead",
                predecessor.id,
                successor.id,
                len(predecessor_candidates),
                [candidate.id for candidate in predecessor_candidates],
            )
            return None

        # A successor whose identity already has its OWN registered actor must
        # never be re-keyed into from here too -- that would orphan or conflict
        # with the actor it already has.
        if self._dispatch_identity(successor) in self._actors:
            return None

        return successor

    async def _continue_actor(
        self,
        predecessor_identity: DispatchActorId,
        predecessor: "ActorDispatcher.ActorAndChannel",
        dispatch: Dispatch,
    ) -> bool:
        """Route `dispatch` to `predecessor`'s actor as a continuation.

        Fall back to ordinary stop/start if the actor cannot safely accept it.

        Args:
            predecessor_identity: The predecessor actor's current identity key.
            predecessor: The predecessor actor-and-channel entry.
            dispatch: The new dispatch continuing the predecessor.

        Returns:
            `True` if the continuation was delivered AND the actor confirmed it
                adopted the new dispatch state and submitted its new proposal
                through the existing pool (the actor is re-indexed under the
                new dispatch's identity). `False` in every other case -- the
                predecessor is NOT left stale: it has already been explicitly
                stopped here, and the caller should start a fresh, independent
                actor for `dispatch` if it hasn't already been stopped as part
                of the caller's own normal flow (see `_stop_actor`, which calls
                this and, on `False`, has nothing further to do since the
                actor is already gone).
        """
        if not isinstance(predecessor.actor, ContinuationCapable):
            _logger.info(
                "Dispatch %s exactly continues dispatch %s, but its actor does "
                "not implement the continuation-acceptance contract "
                "(ContinuationCapable.accept_continuation): stopping it and "
                "starting %s fresh, instead of leaving it running with a stale "
                "payload.",
                dispatch.id,
                predecessor.current_dispatch.id,
                dispatch.id,
            )
            return await self._reject_continuation(predecessor_identity, predecessor)

        continuation_update = DispatchInfo(
            target=dispatch.target,
            dry_run=dispatch.dry_run,
            options=dispatch.payload,
            _src=dispatch,
            continuation_of=predecessor.current_dispatch.id,
        )

        try:
            accepted = await asyncio.wait_for(
                predecessor.actor.accept_continuation(continuation_update),
                timeout=self._continuation_accept_timeout.total_seconds(),
            )
        except TimeoutError:
            _logger.error(
                "Dispatch %s's actor did not respond to a continuation from "
                "dispatch %s within %s; stopping it instead of letting it "
                "stall this service's _run() loop indefinitely, and starting "
                "%s fresh",
                predecessor.current_dispatch.id,
                dispatch.id,
                self._continuation_accept_timeout,
                dispatch.id,
            )
            return await self._reject_continuation(predecessor_identity, predecessor)
        except Exception as e:  # pylint: disable=broad-except
            _logger.error(
                "Failed to deliver continuation update from dispatch %s to "
                "dispatch %s; stopping the predecessor's actor instead of leaving "
                "its proposal orphaned, and starting %s fresh",
                predecessor.current_dispatch.id,
                dispatch.id,
                dispatch.id,
                exc_info=e,
            )
            return await self._reject_continuation(predecessor_identity, predecessor)

        if not accepted:
            _logger.info(
                "Dispatch %s's actor explicitly refused the continuation from "
                "dispatch %s; stopping it and starting %s fresh",
                predecessor.current_dispatch.id,
                dispatch.id,
                dispatch.id,
            )
            return await self._reject_continuation(predecessor_identity, predecessor)

        new_identity = self._dispatch_identity(dispatch)
        del self._actors[predecessor_identity]
        predecessor.current_dispatch = dispatch
        self._actors[new_identity] = predecessor
        _logger.info(
            "Dispatch %s continues dispatch %s: reused its actor instead of "
            "stopping and restarting",
            dispatch.id,
            continuation_update.continuation_of,
        )
        return True

    async def _reject_continuation(
        self,
        predecessor_identity: DispatchActorId,
        predecessor: "ActorDispatcher.ActorAndChannel",
    ) -> bool:
        """Stop and remove the predecessor actor.

        This retains ordinary stop/start behaviour as if no continuation had
        been attempted.

        Args:
            predecessor_identity: The predecessor actor's current identity key.
            predecessor: The predecessor actor-and-channel entry to stop.

        Returns:
            Always `False`, so callers can `return await self._reject_continuation(...)`.
        """
        del self._actors[predecessor_identity]
        await predecessor.actor.stop("continuation not accepted")
        await predecessor.channel.close()
        return False

    async def _start_actor(self, dispatch: Dispatch) -> None:
        """Start the actor the given dispatch refers to.

        Args:
            dispatch: The dispatch to start the actor for.
        """
        dispatch_update = DispatchInfo(
            target=dispatch.target,
            dry_run=dispatch.dry_run,
            options=dispatch.payload,
            _src=dispatch,
        )

        identity = self._dispatch_identity(dispatch)
        actor_and_channel = self._actors.get(identity)

        if actor_and_channel:
            await actor_and_channel.sender.send(dispatch_update)
            actor_and_channel.current_dispatch = dispatch
            _logger.info(
                "Actor for dispatch type %r is already running, "
                "sent a dispatch update instead of creating a new actor",
                dispatch.type,
            )
            return

        candidate = self._find_unique_continuation_candidate(dispatch)
        if candidate is not None:
            predecessor_identity, predecessor = candidate
            if await self._continue_actor(predecessor_identity, predecessor, dispatch):
                return
            # Continuation delivery failed and the predecessor has already been
            # cleaned up (see `_continue_actor`) -- fall through to start fresh.

        try:
            _logger.info("Starting actor for dispatch type %r", dispatch.type)
            channel = Broadcast[DispatchInfo](
                name=f"dispatch_updates_channel_instance={identity}",
                resend_latest=True,
            )
            actor = await self._actor_factory(
                dispatch_update,
                channel.new_receiver(limit=1, warn_on_overflow=False),
            )

            actor.start()

        except Exception as e:  # pylint: disable=broad-except
            _logger.error(
                "Failed to start actor for dispatch type %r",
                dispatch.type,
                exc_info=e,
            )
            self._failed_dispatches[identity] = dispatch
        else:
            # No exception occurred, so we can add the actor to the list
            self._actors[identity] = ActorDispatcher.ActorAndChannel(
                actor=actor,
                channel=channel,
                sender=channel.new_sender(),
                current_dispatch=dispatch,
            )

    async def _stop_actor(self, stopping_dispatch: Dispatch, msg: str) -> None:
        """Stop the actor or continue it into a unique started successor.

        This stop is NEVER withheld or delayed while that is being decided:
        every stop this class receives is one it was always going to receive
        (nothing is suppressed upstream), and the decision below is made
        synchronously, in this one call, using only data already known at
        this instant.

        Args:
            stopping_dispatch: The dispatch that is stopping the actor.
            msg: The message to be passed to the actors being stopped.
        """
        identity = self._dispatch_identity(stopping_dispatch)
        actor_and_channel = self._actors.get(identity)

        if actor_and_channel is None:
            _logger.warning(
                "Actor for dispatch type %r is not running, ignoring stop request",
                stopping_dispatch.type,
            )
            return

        successor = self._find_unique_continuation_successor(stopping_dispatch)
        if successor is not None:
            if await self._continue_actor(identity, actor_and_channel, successor):
                _logger.info(
                    "Dispatch %s's stop resolved into a continuation of "
                    "dispatch %s instead of stopping",
                    stopping_dispatch.id,
                    successor.id,
                )
                return
            # `_continue_actor` already stopped and removed the actor on any
            # failure path (capability check, refusal, timeout, exception) --
            # nothing further to do here.
            return

        del self._actors[identity]
        _logger.info("Stopping actor for dispatch type %r", stopping_dispatch.type)
        await actor_and_channel.actor.stop(msg)
        await actor_and_channel.channel.close()

    async def _run(self) -> None:
        """Run the background service."""
        async for selected in select(self._retry_timer_rx, self._dispatch_rx):
            if self._retry_timer_rx.triggered(selected):
                if not self._failed_dispatches:
                    continue

                _logger.info(
                    "Retrying %d failed actor starts",
                    len(self._failed_dispatches),
                )
                keys = list(self._failed_dispatches.keys())
                for identity in keys:
                    dispatch = self._failed_dispatches[identity]

                    await self._handle_dispatch(dispatch)
            elif self._dispatch_rx.triggered(selected):
                await self._handle_dispatch(selected.message)

    async def _handle_dispatch(self, dispatch: Dispatch) -> None:
        """Process a dispatch to start, update, or stop an actor.

        If a newer version of a previously failed dispatch is received, the
        pending retry for the older version is canceled to ensure only the
        latest dispatch is processed.

        Args:
            dispatch: The dispatch to handle.
        """
        identity = self._dispatch_identity(dispatch)
        if identity in self._failed_dispatches:
            self._failed_dispatches.pop(identity)

        if dispatch.started:
            await self._start_actor(dispatch)
        else:
            await self._stop_actor(dispatch, "Dispatch stopped")
