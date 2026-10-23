# License: All rights reserved
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Helper class to manage actors based on dispatches."""

import asyncio
import logging
from collections.abc import Callable, Mapping
from copy import copy
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Awaitable
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
    """The target components."""

    dry_run: bool
    """Whether this is a dry run."""

    options: dict[str, Any]
    """The additional options."""

    _src: Dispatch
    """The dispatch that triggered this update."""

    def __init__(
        self,
        *,
        target: TargetComponents | None = None,
        components: TargetComponents | None = None,
        dry_run: bool,
        options: dict[str, Any],
        _src: Dispatch,
    ) -> None:
        """Initialize a new instance.

        Args:
            target: The target components to be used.
            components: Deprecated alias for `target`.
            dry_run: Whether this is a dry run.
            options: The additional options.
            _src: The dispatch that triggered this update.

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


class ActorDispatcher(BackgroundService):
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

    @dataclass(frozen=True, kw_only=True)
    class ActorAndChannel:
        """An actor and its dispatch update sender."""

        actor: Actor
        """The actor."""

        channel: Broadcast[DispatchInfo]
        """The channel for dispatch updates."""

        sender: Sender[DispatchInfo]
        """The sender for dispatch updates."""

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
    ) -> None:
        """Initialize the dispatch handler.

        Args:
            actor_factory: A callable that creates an actor with some initial dispatch
                information.
            running_status_receiver: The receiver for dispatch running status changes.
            dispatch_identity: A function to identify to which actor a dispatch refers.
                By default, it uses the dispatch ID.
            retry_interval: How long to wait until trying to start failed actors again.
        """
        super().__init__()
        self._dispatch_identity: Callable[[Dispatch], DispatchActorId] = (
            dispatch_identity if dispatch_identity else lambda d: DispatchActorId(d.id)
        )

        self._dispatch_rx = running_status_receiver
        self._retry_timer_rx = Timer(retry_interval, SkipMissedAndDrift())
        self._actor_factory = actor_factory
        self._actors: dict[DispatchActorId, ActorDispatcher.ActorStatus] = {}
        self._dispatch_status: dict[DispatchId, ActorDispatcher.DispatchStatus] = {}
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
            _logger.info(
                "Actor for dispatch type %r is already running, "
                "sent a dispatch update instead of creating a new actor",
                dispatch.type,
            )
        else:
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
                    )
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
                        dispatch,
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
