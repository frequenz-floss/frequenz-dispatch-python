# License: All rights reserved
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Helper class to manage actors based on dispatches."""

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Awaitable

from frequenz.channels import Broadcast, Receiver, Sender, select
from frequenz.client.dispatch.types import TargetComponents
from frequenz.sdk.actor import Actor, BackgroundService

from ._dispatch import Dispatch

_logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class DispatchInfo:
    """Event emitted when the dispatch changes."""

    components: TargetComponents
    """Components to be used."""

    dry_run: bool
    """Whether this is a dry run."""

    options: dict[str, Any]
    """Additional options."""

    _src: Dispatch
    """The dispatch that triggered this update."""


class ActorDispatcher(BackgroundService):
    """Helper class to manage actors based on dispatches.

    Example usage:

    ```python
    import os
    import asyncio
    from typing import override
    from frequenz.dispatch import Dispatcher, ActorDispatcher, DispatchInfo
    from frequenz.client.dispatch.types import TargetComponents
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
        ) -> "Self":
            self = cls(name=name)
            self._dispatch_updates_receiver = dispatch_updates_receiver
            self._update_dispatch_information(initial_dispatch)
            return self

        @override
        async def _run(self) -> None:
            other_recv: Receiver[Any] = ...

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
            match dispatch_update.components:
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
        key  = os.getenv("DISPATCH_API_KEY", "some-key")

        microgrid_id = 1

        async with Dispatcher(
            microgrid_id=microgrid_id,
            server_url=url,
            key=key
        ) as dispatcher:
            status_receiver = dispatcher.new_running_state_event_receiver("EXAMPLE_TYPE")

            managing_actor = ActorDispatcher(
                actor_factory=MyActor.new_with_dispatch,
                running_status_receiver=status_receiver,
            )

            await run(managing_actor)
    ```
    """

    class FailedDispatchesRetrier(BackgroundService):
        """Manages the retring of failed dispatches."""

        def __init__(self, retry_interval: timedelta) -> None:
            """Initialize the retry manager.

            Args:
                retry_interval: The interval between retries.
            """
            super().__init__()
            self._retry_interval = retry_interval
            self._channel = Broadcast[Dispatch](name="retry_channel")
            self._sender = self._channel.new_sender()

        def start(self) -> None:
            """Start the background service.

            This is a no-op.
            """

        def new_receiver(self) -> Receiver[Dispatch]:
            """Create a new receiver for dispatches to retry.

            Returns:
                The receiver.
            """
            return self._channel.new_receiver()

        def retry(self, dispatch: Dispatch) -> None:
            """Retry a dispatch.

            Args:
                dispatch: The dispatch information to retry.
            """
            task = asyncio.create_task(self._retry_after_delay(dispatch))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.remove)

        async def _retry_after_delay(self, dispatch: Dispatch) -> None:
            """Retry a dispatch after a delay.

            Args:
                dispatch: The dispatch information to retry.
            """
            _logger.info(
                "Will retry dispatch %s after %s",
                dispatch.id,
                self._retry_interval,
            )
            await asyncio.sleep(self._retry_interval.total_seconds())
            _logger.info("Retrying dispatch %s now", dispatch.id)
            await self._sender.send(dispatch)

    @dataclass(frozen=True, kw_only=True)
    class ActorAndChannel:
        """Actor and its sender."""

        actor: Actor
        """The actor."""

        channel: Broadcast[DispatchInfo]
        """The channel for dispatch updates."""

        sender: Sender[DispatchInfo]
        """The sender for dispatch updates."""

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        actor_factory: Callable[
            [DispatchInfo, Receiver[DispatchInfo]], Awaitable[Actor]
        ],
        running_status_receiver: Receiver[Dispatch],
        dispatch_identity: Callable[[Dispatch], int] | None = None,
        retry_interval: timedelta = timedelta(seconds=60),
    ) -> None:
        """Initialize the dispatch handler.

        Args:
            actor_factory: A callable that creates an actor with some initial dispatch
                information.
            running_status_receiver: The receiver for dispatch running status changes.
            dispatch_identity: A function to identify to which actor a dispatch refers.
                By default, it uses the dispatch ID.
            retry_interval: The interval between retries.
        """
        super().__init__()
        self._dispatch_identity: Callable[[Dispatch], int] = (
            dispatch_identity if dispatch_identity else lambda d: d.id
        )

        self._dispatch_rx = running_status_receiver
        self._actor_factory = actor_factory
        self._actors: dict[int, ActorDispatcher.ActorAndChannel] = {}

        self._retrier = ActorDispatcher.FailedDispatchesRetrier(retry_interval)

    def start(self) -> None:
        """Start the background service."""
        self._tasks.add(asyncio.create_task(self._run()))

    async def _start_actor(self, dispatch: Dispatch) -> None:
        """Start the actor the given dispatch refers to."""
        dispatch_update = DispatchInfo(
            components=dispatch.target,
            dry_run=dispatch.dry_run,
            options=dispatch.payload,
            _src=dispatch,
        )

        identity = self._dispatch_identity(dispatch)
        actor_and_channel = self._actors.get(identity)

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
                self._retrier.retry(dispatch)
            else:
                # No exception occurred, so we can add the actor to the list
                self._actors[identity] = ActorDispatcher.ActorAndChannel(
                    actor=actor, channel=channel, sender=channel.new_sender()
                )

    async def _stop_actor(self, stopping_dispatch: Dispatch, msg: str) -> None:
        """Stop all actors.

        Args:
            stopping_dispatch: The dispatch that is stopping the actor.
            msg: The message to be passed to the actors being stopped.
        """
        identity = self._dispatch_identity(stopping_dispatch)

        if actor_and_channel := self._actors.pop(identity, None):
            await actor_and_channel.actor.stop(msg)
            await actor_and_channel.channel.close()
        else:
            _logger.warning(
                "Actor for dispatch type %r is not running", stopping_dispatch.type
            )

    async def _run(self) -> None:
        """Run the background service."""
        async with self._retrier:
            retry_recv = self._retrier.new_receiver()

            async for selected in select(retry_recv, self._dispatch_rx):
                if retry_recv.triggered(selected):
                    self._retrier.retry(selected.message)
                elif self._dispatch_rx.triggered(selected):
                    await self._handle_dispatch(selected.message)

    async def _handle_dispatch(self, dispatch: Dispatch) -> None:
        """Handle a dispatch.

        Args:
            dispatch: The dispatch to handle.
        """
        if dispatch.started:
            await self._start_actor(dispatch)
        else:
            await self._stop_actor(dispatch, "Dispatch stopped")
