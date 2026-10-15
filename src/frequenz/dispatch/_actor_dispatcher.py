# License: All rights reserved
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Helper class to manage actors based on dispatches.

## Continuation handovers

By default, when one dispatch stops and a different dispatch starts,
`ActorDispatcher` always stops the old actor and starts a new one, even if
the two dispatches are for the exact same target, back-to-back in time. For
an actor that holds some running state (a power proposal, an open
connection), that stop/start pair is wasted work, and the small gap between
stop and start can matter.

A continuation handover lets `ActorDispatcher` reuse the SAME actor for a new
dispatch instead, when the new dispatch is an exact, compatible continuation
of the one the actor is already running under: same type, same target, same
`dry_run` mode, and starting exactly when the predecessor ends.

This is entirely opt-in and changes nothing by default:

* The caller must pass `continuation_predicate` to
  `Dispatcher.start_managing()` (normally `is_exact_continuation`).
* The actor's class must implement `ContinuationCapable`.

When both are true and exactly one matching dispatch is found, the actor
receives a `DispatchInfo` with `continuation_of` set, through
`ContinuationCapable.accept_continuation()`, instead of being stopped. In
every other case -- the predicate isn't set, the actor doesn't implement
`ContinuationCapable`, the match is ambiguous, the actor refuses, or
`accept_continuation()` raises or times out -- `ActorDispatcher` falls back
to the normal stop/start.

Example:
    ```python
    from typing import Self

    from frequenz.channels import Receiver
    from frequenz.client.dispatch.types import DispatchId
    from frequenz.dispatch import Dispatcher, DispatchInfo, is_exact_continuation
    from frequenz.sdk.actor import Actor

    class MyActor(Actor):
        def __init__(
            self, current_dispatch_id: DispatchId, *, name: str | None = None
        ) -> None:
            super().__init__(name=name)
            self.current_dispatch_id = current_dispatch_id

        @classmethod
        async def new_with_dispatch(
            cls, initial_dispatch: DispatchInfo, _receiver: Receiver[DispatchInfo]
        ) -> Self:
            return cls(initial_dispatch.dispatch.id)

        async def accept_continuation(self, update: DispatchInfo) -> bool:
            if update.continuation_of != self.current_dispatch_id:
                return False  # Not continuing the dispatch we're running.
            self.current_dispatch_id = update.dispatch.id
            # Adopt the new dispatch's target/options here.
            return True

        async def _run(self) -> None:
            pass  # Adopt the initial dispatch's target/options here.

    async def manage(dispatcher: Dispatcher) -> None:
        await dispatcher.start_managing(
            "MY_TYPE",
            actor_factory=MyActor.new_with_dispatch,
            continuation_predicate=is_exact_continuation,
        )
    ```
"""

import asyncio
import enum
import logging
from collections.abc import Callable, Mapping
from copy import copy
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
from ._merge_helpers import _other_running_dispatches

_logger = logging.getLogger(__name__)


class _ContinuationOutcome(enum.Enum):
    """The result of trying to hand a dispatch over to a predecessor's actor."""

    ACCEPTED = enum.auto()
    """The actor adopted the new dispatch; it's now running under its identity."""

    REFUSED = enum.auto()
    """The actor explicitly declined; it keeps running under its own dispatch."""

    UNKNOWN = enum.auto()
    """`accept_continuation()` timed out or raised; the actor's state is unknown."""


@runtime_checkable
class ContinuationCapable(Protocol):
    """An actor that can accept or refuse a continuation update.

    `ActorDispatcher` only reuses a running actor for a new, unrelated
    dispatch (see `continuation_predicate`) when the actor's class implements
    this protocol. An actor that does not implement it is never reused: a
    fresh actor is started for the new dispatch instead, exactly as if no
    continuation had been attempted.
    """

    async def accept_continuation(self, update: "DispatchInfo") -> bool:
        """Accept or refuse a continuation update.

        If this returns `True`, the actor must already be acting on
        `update.dispatch` as its new dispatch before returning.

        Args:
            update: The continuation update. `update.continuation_of` is the
                dispatch id this actor is currently running under, and
                `update.dispatch` is the new dispatch to adopt.

        Returns:
            `True` to accept `update` and adopt it as the current dispatch.
            `False` to refuse; the caller then starts a separate, fresh actor
                for the new dispatch and leaves this one running as before.
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
            Use [`target`][frequenz.dispatch.DispatchInfo.target] instead.
        """
        return self.target

    target: TargetComponents
    """The target components."""

    dry_run: bool
    """Whether this is a dry run."""

    options: dict[str, Any]
    """The additional options."""

    _src: Dispatch
    """The dispatch that triggered this update."""

    @property
    def dispatch(self) -> Dispatch:
        """The dispatch that triggered this update.

        A public alias for the same dispatch the private `_src` field holds,
        so an actor's `accept_continuation` can read the new dispatch (for
        example, to get its id or payload) without reaching into a private
        attribute.
        """
        return self._src

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
        """Initialize a new instance.

        Args:
            target: The target components to be used.
            components: Deprecated alias for `target`.
            dry_run: Whether this is a dry run.
            options: The additional options.
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
                other_recv: Receiver[Any] = Broadcast[Any](name="other").new_receiver()

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

    @dataclass(frozen=True, kw_only=True)
    class ActorStatus:
        """The running actor or failed dispatch for one actor identity."""

        actor_and_channel: "ActorDispatcher.ActorAndChannel | None" = None
        failed_dispatch: Dispatch | None = None

    @dataclass(frozen=True, kw_only=True)
    class DispatchStatus:
        """The latest dispatch and identity routed for one dispatch ID."""

        dispatch: Dispatch
        identity: DispatchActorId

    @dataclass(frozen=True, kw_only=True)
    class MergeContext:
        """The merge filter and the dispatches it evaluates."""

        dispatch_filter: Callable[[Mapping[DispatchId, Dispatch], Dispatch], bool]
        known_dispatches: Mapping[DispatchId, Dispatch]

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

        `continuation_predicate` and `dispatches_snapshot` are both for
        continuation handovers -- see the module docstring. They can only be
        enabled through `Dispatcher.start_managing()`, which supplies a
        matching pair of both; building this class directly with only one of
        them is an error.

        Args:
            actor_factory: A callable that creates an actor with some initial dispatch
                information.
            running_status_receiver: The receiver for dispatch running status changes.
            dispatch_identity: A function to identify to which actor a dispatch refers.
                By default, it uses the dispatch ID.
            retry_interval: How long to wait until trying to start failed actors again.
            continuation_predicate: Opt-in, default `None`. Called as
                `continuation_predicate(current_dispatch, new_dispatch)` to
                decide whether a newly-started dispatch continues the
                dispatch an existing actor is already running under. Normally
                `is_exact_continuation`.
            dispatches_snapshot: Opt-in, default `None`. A zero-argument
                callable returning a live, read-only view of every dispatch
                currently known, used together with `continuation_predicate`
                to confirm a unique predecessor/successor pair.
            continuation_accept_timeout: How long to wait for a candidate
                actor's `accept_continuation()` before treating it as a
                refusal.

        Raises:
            ValueError: If exactly one of `continuation_predicate` and
                `dispatches_snapshot` is given.
        """
        if (continuation_predicate is None) != (dispatches_snapshot is None):
            raise ValueError(
                "continuation_predicate and dispatches_snapshot must be given "
                "together, or not at all. Continuations can only be enabled "
                "through Dispatcher.start_managing()."
            )

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
        self._actors: dict[DispatchActorId, ActorDispatcher.ActorStatus] = {}
        self._dispatch_status: dict[DispatchId, ActorDispatcher.DispatchStatus] = {}
        self._recently_continued: set[DispatchActorId] = set()
        """Identities whose actor was just re-keyed away by a continuation.

        The predecessor dispatch's own stop event still arrives later and
        finds no actor at its identity -- expected, so it's logged at DEBUG
        instead of WARNING. Each identity is removed from this set the first
        time that happens, so it can't grow without bound.
        """
        self._merge_context: ActorDispatcher.MergeContext | None = None

    def start(self) -> None:
        """Start the background service."""
        self._tasks.add(asyncio.create_task(self._run()))

    def _set_merge_context(
        self,
        dispatch_filter: Callable[[Mapping[DispatchId, Dispatch], Dispatch], bool],
        known_dispatches: Mapping[DispatchId, Dispatch],
    ) -> None:
        """Set the merge filter and its complete dispatch context."""
        self._merge_context = ActorDispatcher.MergeContext(
            dispatch_filter=dispatch_filter,
            known_dispatches=known_dispatches,
        )

    def _find_unique_continuation_candidate(
        self, dispatch: Dispatch
    ) -> tuple[DispatchActorId, "ActorDispatcher.ActorAndChannel"] | None:
        """Find the single running actor `dispatch` exactly continues, if any.

        Args:
            dispatch: The newly-arrived dispatch, not already matched by
                `dispatch_identity` to an existing actor.

        Returns:
            The `(identity, actor_and_channel)` of the unique continuation
            pair, or `None` if either side of the pair is not unique, or no
            running actor implementing `ContinuationCapable` matches.
        """
        if self._continuation_predicate is None or self._dispatches_snapshot is None:
            return None

        candidates = [
            (identity, actor_status.actor_and_channel)
            for identity, actor_status in self._actors.items()
            if actor_status.actor_and_channel is not None
            and isinstance(actor_status.actor_and_channel.actor, ContinuationCapable)
            and self._continuation_predicate(
                actor_status.actor_and_channel.current_dispatch, dispatch
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

    async def _continue_actor(
        self,
        predecessor_identity: DispatchActorId,
        predecessor: "ActorDispatcher.ActorAndChannel",
        dispatch: Dispatch,
    ) -> "_ContinuationOutcome":
        """Try to route `dispatch` to `predecessor`'s actor as a continuation.

        The caller already verified `predecessor.actor` implements
        `ContinuationCapable` (see `_find_unique_continuation_candidate`).

        Args:
            predecessor_identity: The predecessor actor's current identity key.
            predecessor: The predecessor actor-and-channel entry.
            dispatch: The new dispatch continuing the predecessor.

        Returns:
            `ACCEPTED` if the actor adopted `dispatch` and was re-indexed
                under its identity; the caller does nothing more.
            `REFUSED` if the actor explicitly declined. It is left running
                exactly as before: it correctly handled the update, and its
                own stop event will stop it normally when it arrives. The
                caller must still start a fresh actor for `dispatch`.
            `UNKNOWN` if `accept_continuation()` timed out or raised. The
                actor's state can no longer be trusted. The caller must
                start a fresh actor for `dispatch` FIRST, then stop this
                predecessor (`_discard_unresponsive_predecessor`), so the
                target is never left without an actor for longer than
                necessary.
        """
        # The caller only ever finds this predecessor through
        # `_find_unique_continuation_candidate`, which already checked this.
        assert isinstance(predecessor.actor, ContinuationCapable)

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
                "dispatch %s within %s; starting %s fresh and then stopping "
                "the predecessor, since its state can no longer be trusted",
                predecessor.current_dispatch.id,
                dispatch.id,
                self._continuation_accept_timeout,
                dispatch.id,
            )
            return _ContinuationOutcome.UNKNOWN
        except Exception as e:  # pylint: disable=broad-except
            _logger.error(
                "Failed to deliver a continuation update from dispatch %s to "
                "dispatch %s; starting %s fresh and then stopping the "
                "predecessor, since its state can no longer be trusted",
                predecessor.current_dispatch.id,
                dispatch.id,
                dispatch.id,
                exc_info=e,
            )
            return _ContinuationOutcome.UNKNOWN

        if not accepted:
            _logger.info(
                "Dispatch %s's actor explicitly refused the continuation from "
                "dispatch %s; starting %s fresh and leaving the predecessor "
                "running for its own stop event",
                predecessor.current_dispatch.id,
                dispatch.id,
                dispatch.id,
            )
            return _ContinuationOutcome.REFUSED

        new_identity = self._dispatch_identity(dispatch)
        del self._actors[predecessor_identity]
        predecessor.current_dispatch = dispatch
        self._actors[new_identity] = ActorDispatcher.ActorStatus(
            actor_and_channel=predecessor
        )
        self._recently_continued.add(predecessor_identity)
        _logger.info(
            "Dispatch %s continues dispatch %s: reused its actor instead of "
            "stopping and restarting",
            dispatch.id,
            continuation_update.continuation_of,
        )
        return _ContinuationOutcome.ACCEPTED

    async def _discard_unresponsive_predecessor(
        self,
        predecessor_identity: DispatchActorId,
        predecessor: "ActorDispatcher.ActorAndChannel",
    ) -> None:
        """Stop and remove a predecessor whose continuation outcome is unknown.

        Only used for `_ContinuationOutcome.UNKNOWN` (a timeout or an
        exception): the actor's internal state can't be trusted, so it's
        stopped defensively. An explicit `REFUSED` does NOT go through this
        -- that actor correctly declined and is left running for its own,
        normal stop event.

        Args:
            predecessor_identity: The predecessor actor's current identity key.
            predecessor: The predecessor actor-and-channel entry to stop.
        """
        del self._actors[predecessor_identity]
        await predecessor.actor.stop("continuation outcome unknown")
        await predecessor.channel.close()

    async def _try_handover(
        self, dispatch: Dispatch
    ) -> tuple[bool, tuple[DispatchActorId, "ActorDispatcher.ActorAndChannel"] | None]:
        """Try a continuation handover for `dispatch`.

        Args:
            dispatch: The newly-arrived dispatch that may continue a
                running actor's predecessor.

        Returns:
            `(True, None)` if the handover succeeded; `_start_actor` does
                nothing more. `(False, predecessor)` if `_start_actor` must
                start a fresh actor, where `predecessor` is `None` (no
                candidate, or an explicit refusal) or the predecessor to
                discard AFTER starting the fresh actor (a timeout or an
                exception, see `_continue_actor`).
        """
        candidate = self._find_unique_continuation_candidate(dispatch)
        if candidate is None:
            return False, None

        predecessor_identity, predecessor = candidate
        outcome = await self._continue_actor(
            predecessor_identity, predecessor, dispatch
        )
        if outcome is _ContinuationOutcome.ACCEPTED:
            return True, None
        if outcome is _ContinuationOutcome.UNKNOWN:
            return False, (predecessor_identity, predecessor)
        return False, None  # REFUSED: left running for its own stop event.

    async def _start_actor(self, dispatch: Dispatch, identity: DispatchActorId) -> None:
        """Start the actor the given dispatch refers to.

        Args:
            dispatch: The dispatch to start the actor for.
            identity: The identity to route the dispatch to.
        """
        dispatch_update = DispatchInfo(
            target=dispatch.target,
            dry_run=dispatch.dry_run,
            options=dispatch.payload,
            _src=dispatch,
        )

        actor_status = self._actors.get(identity)
        actor_and_channel = (
            actor_status.actor_and_channel if actor_status is not None else None
        )

        if actor_and_channel:
            await actor_and_channel.sender.send(dispatch_update)
            actor_and_channel.current_dispatch = dispatch
            _logger.info(
                "Actor for dispatch type %r is already running, "
                "sent a dispatch update instead of creating a new actor",
                dispatch.type,
            )
            return

        handed_over, predecessor_to_discard = await self._try_handover(dispatch)
        if handed_over:
            return

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
            self._actors[identity] = ActorDispatcher.ActorStatus(
                failed_dispatch=dispatch
            )
        else:
            # No exception occurred, so we can add the actor to the list
            self._actors[identity] = ActorDispatcher.ActorStatus(
                actor_and_channel=ActorDispatcher.ActorAndChannel(
                    actor=actor,
                    channel=channel,
                    sender=channel.new_sender(),
                    current_dispatch=dispatch,
                )
            )

        if predecessor_to_discard is not None:
            discard_identity, discard_actor_and_channel = predecessor_to_discard
            await self._discard_unresponsive_predecessor(
                discard_identity, discard_actor_and_channel
            )

    async def _stop_actor(
        self,
        stopping_dispatch: Dispatch,
        identity: DispatchActorId,
        msg: str,
    ) -> None:
        """Stop the actor for the given dispatch.

        Args:
            stopping_dispatch: The dispatch that is stopping the actor.
            identity: The identity of the actor to stop.
            msg: The message to be passed to the actors being stopped.
        """
        actor_status = self._actors.pop(identity, None)
        actor_and_channel = (
            actor_status.actor_and_channel if actor_status is not None else None
        )
        if actor_and_channel is not None:
            _logger.info("Stopping actor for dispatch type %r", stopping_dispatch.type)
            await actor_and_channel.actor.stop(msg)
            await actor_and_channel.channel.close()
        elif identity in self._recently_continued:
            # A continuation handover already moved this identity's actor
            # elsewhere (see `_continue_actor`); its own stop arriving here
            # and finding nothing is expected, not a problem.
            self._recently_continued.discard(identity)
            _logger.debug(
                "Actor for dispatch type %r was already handed over to a "
                "continuation, ignoring its stop request",
                stopping_dispatch.type,
            )
        else:
            _logger.warning(
                "Actor for dispatch type %r is not running, ignoring stop request",
                stopping_dispatch.type,
            )

    async def _run(self) -> None:
        """Run the background service."""
        async for selected in select(self._retry_timer_rx, self._dispatch_rx):
            if self._retry_timer_rx.triggered(selected):
                failed_dispatches = [
                    actor_status.failed_dispatch
                    for actor_status in self._actors.values()
                    if actor_status.failed_dispatch is not None
                ]
                if not failed_dispatches:
                    continue

                _logger.info(
                    "Retrying %d failed actor starts",
                    len(failed_dispatches),
                )
                for dispatch in failed_dispatches:
                    await self._handle_dispatch(dispatch)
            elif self._dispatch_rx.triggered(selected):
                await self._handle_dispatch(selected.message)

    def _should_stop_actor(
        self,
        dispatch: Dispatch,
        identity: DispatchActorId,
        *,
        check_local_dispatches: bool = False,
    ) -> bool:
        """Return whether the actor for an identity should stop."""
        if self._merge_context is not None:
            return self._merge_context.dispatch_filter(
                self._merge_context.known_dispatches, dispatch
            )

        if not check_local_dispatches:
            return True

        return not _other_running_dispatches(
            {
                dispatch_id: status.dispatch
                for dispatch_id, status in self._dispatch_status.items()
            },
            dispatch.id,
            identity,
            self._dispatch_identity,
        )

    async def _handle_dispatch(self, dispatch: Dispatch) -> None:
        """Process a dispatch to start, update, or stop an actor.

        If a newer version of a previously failed dispatch is received, the
        pending retry for the older version is canceled to ensure only the
        latest dispatch is processed.

        Args:
            dispatch: The dispatch to handle.
        """
        if dispatch.started:
            identity = self._dispatch_identity(dispatch)
            previous_status = self._dispatch_status.get(dispatch.id)
            previous_identity = (
                previous_status.identity if previous_status is not None else None
            )

            if previous_identity is not None and previous_identity != identity:
                assert previous_status is not None
                actor_status = self._actors.get(previous_identity)
                failed_dispatch = (
                    actor_status.failed_dispatch if actor_status is not None else None
                )
                if failed_dispatch is not None and failed_dispatch.id == dispatch.id:
                    self._actors.pop(previous_identity)

                stopping_dispatch = copy(previous_status.dispatch)
                # pylint: disable-next=protected-access
                stopping_dispatch._set_deleted()
                if self._should_stop_actor(
                    stopping_dispatch,
                    previous_identity,
                    check_local_dispatches=True,
                ):
                    await self._stop_actor(
                        stopping_dispatch,
                        previous_identity,
                        "Dispatch identity changed",
                    )

            self._dispatch_status[dispatch.id] = ActorDispatcher.DispatchStatus(
                dispatch=dispatch,
                identity=identity,
            )
            actor_status = self._actors.get(identity)
            if actor_status is not None and actor_status.failed_dispatch is not None:
                self._actors.pop(identity)
            await self._start_actor(dispatch, identity)
        else:
            dispatch_status = self._dispatch_status.pop(dispatch.id, None)
            identity = (
                dispatch_status.identity
                if dispatch_status is not None
                else self._dispatch_identity(dispatch)
            )

            actor_status = self._actors.get(identity)
            failed_dispatch = (
                actor_status.failed_dispatch if actor_status is not None else None
            )
            if failed_dispatch is not None and failed_dispatch.id == dispatch.id:
                self._actors.pop(identity)

            if self._should_stop_actor(dispatch, identity):
                await self._stop_actor(dispatch, identity, "Dispatch stopped")
