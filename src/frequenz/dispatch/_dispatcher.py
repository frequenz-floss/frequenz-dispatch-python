# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""A highlevel interface for the dispatch API."""

from __future__ import annotations

import asyncio
import logging
from asyncio import Event
from datetime import timedelta
from typing import Awaitable, Callable, Self

from frequenz.channels import Receiver
from frequenz.client.dispatch import DispatchApiClient
from frequenz.sdk.actor import Actor, BackgroundService
from typing_extensions import override

from ._actor_dispatcher import ActorDispatcher, DispatchInfo
from ._bg_service import DispatchScheduler, MergeStrategy
from ._dispatch import Dispatch
from ._event import DispatchEvent

_logger = logging.getLogger(__name__)


class Dispatcher(BackgroundService):
    """A highlevel interface for the dispatch API.

    This class provides a highlevel interface to the dispatch API.
    It provides receivers for various events and management of actors based on
    dispatches.

    The receivers shortly explained:

    * [Lifecycle events receiver][frequenz.dispatch.Dispatcher.new_lifecycle_events_receiver]:
        Receives an event whenever a dispatch is created, updated or deleted.
    * [Running status change
        receiver][frequenz.dispatch.Dispatcher.new_running_state_event_receiver]:
        Receives an event whenever the running status of a dispatch changes.
        The running status of a dispatch can change due to a variety of reasons,
        such as but not limited to the dispatch being started, stopped, modified
        or deleted or reaching its scheduled start or end time.

        Any change that could potentially require the consumer to start, stop or
        reconfigure itself will cause a message to be sent.

    Example: Managing an actor
        ```python
        import os
        from frequenz.dispatch import Dispatcher, MergeByType
        from unittest.mock import MagicMock

        async def create_actor(dispatch: DispatchInfo, receiver: Receiver[DispatchInfo]) -> Actor:
            return MagicMock(dispatch=dispatch, receiver=receiver)

        async def run():
            url = os.getenv("DISPATCH_API_URL", "grpc://dispatch.url.goes.here.example.com")
            key  = os.getenv("DISPATCH_API_KEY", "some-key")

            microgrid_id = 1

            async with Dispatcher(
                microgrid_id=microgrid_id,
                server_url=url,
                key=key
            ) as dispatcher:
                dispatcher.start_managing(
                    dispatch_type="DISPATCH_TYPE",
                    actor_factory=create_actor,
                    merge_strategy=MergeByType(),
                )

                await dispatcher
        ```

    Example: Processing running state change dispatches
        ```python
        import os
        from frequenz.dispatch import Dispatcher
        from unittest.mock import MagicMock

        async def run():
            url = os.getenv("DISPATCH_API_URL", "grpc://dispatch.url.goes.here.example.com")
            key  = os.getenv("DISPATCH_API_KEY", "some-key")

            microgrid_id = 1

            async with Dispatcher(
                microgrid_id=microgrid_id,
                server_url=url,
                key=key
            ) as dispatcher:
                actor = MagicMock() # replace with your actor

                rs_receiver = dispatcher.new_running_state_event_receiver("DISPATCH_TYPE")

                async for dispatch in rs_receiver:
                    if dispatch.started:
                        print(f"Executing dispatch {dispatch.id}, due on {dispatch.start_time}")
                        if actor.is_running:
                            actor.reconfigure(
                                components=dispatch.target,
                                run_parameters=dispatch.payload, # custom actor parameters
                                dry_run=dispatch.dry_run,
                                until=dispatch.until,
                            )  # this will reconfigure the actor
                        else:
                            # this will start a new actor with the given components
                            # and run it for the duration of the dispatch
                            actor.start(
                                components=dispatch.target,
                                run_parameters=dispatch.payload, # custom actor parameters
                                dry_run=dispatch.dry_run,
                                until=dispatch.until,
                            )
                    else:
                        actor.stop()  # this will stop the actor
        ```

    Example: Getting notification about dispatch lifecycle events
        ```python
        import os
        from typing import assert_never

        from frequenz.dispatch import Created, Deleted, Dispatcher, Updated

        async def run():
            url = os.getenv("DISPATCH_API_URL", "grpc://dispatch.url.goes.here.example.com")
            key  = os.getenv("DISPATCH_API_KEY", "some-key")

            microgrid_id = 1

            async with Dispatcher(
                microgrid_id=microgrid_id,
                server_url=url,
                key=key,
            ) as dispatcher:
                events_receiver = dispatcher.new_lifecycle_events_receiver("DISPATCH_TYPE")

                async for event in events_receiver:
                    match event:
                        case Created(dispatch):
                            print(f"A dispatch was created: {dispatch}")
                        case Deleted(dispatch):
                            print(f"A dispatch was deleted: {dispatch}")
                        case Updated(dispatch):
                            print(f"A dispatch was updated: {dispatch}")
                        case _ as unhandled:
                            assert_never(unhandled)
        ```

    Example: Creating a new dispatch and then modifying it.
        Note that this uses the lower-level `DispatchApiClient` class to create
        and update the dispatch.

        ```python
        import os
        from datetime import datetime, timedelta, timezone

        from frequenz.client.common.microgrid.components import ComponentCategory

        from frequenz.dispatch import Dispatcher

        async def run():
            url = os.getenv("DISPATCH_API_URL", "grpc://dispatch.url.goes.here.example.com")
            key  = os.getenv("DISPATCH_API_KEY", "some-key")

            microgrid_id = 1

            async with Dispatcher(
                microgrid_id=microgrid_id,
                server_url=url,
                key=key,
            ) as dispatcher:
                # Create a new dispatch
                new_dispatch = await dispatcher.client.create(
                    microgrid_id=microgrid_id,
                    type="ECHO_FREQUENCY",  # replace with your own type
                    start_time=datetime.now(tz=timezone.utc) + timedelta(minutes=10),
                    duration=timedelta(minutes=5),
                    target=ComponentCategory.INVERTER,
                    payload={"font": "Times New Roman"},  # Arbitrary payload data
                )

                # Modify the dispatch
                await dispatcher.client.update(
                    microgrid_id=microgrid_id,
                    dispatch_id=new_dispatch.id,
                    new_fields={"duration": timedelta(minutes=10)}
                )

                # Validate the modification
                modified_dispatch = await dispatcher.client.get(
                    microgrid_id=microgrid_id, dispatch_id=new_dispatch.id
                )
                assert modified_dispatch.duration == timedelta(minutes=10)
        ```
    """

    def __init__(
        self,
        *,
        microgrid_id: int,
        server_url: str,
        key: str,
    ):
        """Initialize the dispatcher.

        Args:
            microgrid_id: The microgrid id.
            server_url: The URL of the dispatch service.
            key: The key to access the service.
        """
        super().__init__()

        self._client = DispatchApiClient(server_url=server_url, key=key)
        self._bg_service = DispatchScheduler(
            microgrid_id,
            self._client,
        )
        self._actor_dispatchers: dict[str, ActorDispatcher] = {}
        self._empty_event = Event()
        self._disconnecting_future: asyncio.Future[None] | None = None

    @override
    def start(self) -> None:
        """Start the local dispatch service."""
        self._bg_service.start()
        self._empty_event.set()

    @property
    @override
    def is_running(self) -> bool:
        """Whether the local dispatch service is running."""
        return self._bg_service.is_running

    @override
    async def wait(self) -> None:
        """Wait until all actor dispatches are stopped and client is disconnected."""
        if self._disconnecting_future is not None:
            await self._disconnecting_future

        await asyncio.gather(self._bg_service.wait(), self._empty_event.wait())
        self._actor_dispatchers.clear()

    def cancel(self, msg: str | None = None) -> None:
        """Stop the local dispatch service and initiate client disconnection."""
        self._bg_service.cancel(msg)

        for instance in self._actor_dispatchers.values():
            instance.cancel()

        # Initiate client disconnection asynchronously
        self._disconnecting_future = asyncio.ensure_future(self._client.disconnect())

    async def wait_for_initialization(self) -> None:
        """Wait until the background service is initialized."""
        await self._bg_service.wait_for_initialization()

    def is_managed(self, dispatch_type: str) -> bool:
        """Check if the dispatcher is managing actors for a given dispatch type.

        Args:
            dispatch_type: The type of the dispatch to check.

        Returns:
            True if the dispatcher is managing actors for the given dispatch type.
        """
        return dispatch_type in self._actor_dispatchers

    async def start_managing(
        self,
        dispatch_type: str,
        *,
        actor_factory: Callable[
            [DispatchInfo, Receiver[DispatchInfo]], Awaitable[Actor]
        ],
        merge_strategy: MergeStrategy | None = None,
        retry_interval: timedelta = timedelta(seconds=60),
    ) -> None:
        """Manage actors for a given dispatch type.

        Creates and manages an
        [`ActorDispatcher`][frequenz.dispatch.ActorDispatcher] for the given type that will
        start, stop and reconfigure actors based on received dispatches.

        You can await the `Dispatcher` instance to block until all types
        registered with `start_managing()` are stopped using
        `stop_managing()`

        "Merging" means that when multiple dispatches are active at the same time,
        the intervals are merged into one.

        This also decides how instances are mapped from dispatches to actors:

        * [`MergeByType`][frequenz.dispatch.MergeByType] — All dispatches map to
        one single instance identified by the dispatch type.
        * [`MergeByTypeTarget`][frequenz.dispatch.MergeByTypeTarget] — A
        dispatch maps to an instance identified by the dispatch type and target.
        So different dispatches with equal type and target will map to the same
        instance.
        * `None` — No merging, each dispatch maps to a separate instance.

        Args:
            dispatch_type: The type of the dispatch to manage.
            actor_factory: The factory to create actors.
            merge_strategy: The strategy to merge running intervals.
            retry_interval: Retry interval for when actor creation fails.
        """
        dispatcher = self._actor_dispatchers.get(dispatch_type)

        if dispatcher is not None:
            _logger.debug(
                "Ignoring duplicate actor dispatcher request for %r", dispatch_type
            )
            return

        self._empty_event.clear()

        def id_identity(dispatch: Dispatch) -> int:
            return dispatch.id

        dispatcher = ActorDispatcher(
            actor_factory=actor_factory,
            running_status_receiver=await self.new_running_state_event_receiver(
                dispatch_type,
                merge_strategy=merge_strategy,
            ),
            dispatch_identity=(
                id_identity if merge_strategy is None else merge_strategy.identity
            ),
            retry_interval=retry_interval,
        )

        self._actor_dispatchers[dispatch_type] = dispatcher
        dispatcher.start()

    async def stop_managing(self, dispatch_type: str) -> None:
        """Stop managing actors for a given dispatch type.

        Args:
            dispatch_type: The type of the dispatch to stop managing.
        """
        dispatcher = self._actor_dispatchers.pop(dispatch_type, None)
        if dispatcher is not None:
            await dispatcher.stop()

        if not self._actor_dispatchers:
            self._empty_event.set()

    @property
    def client(self) -> DispatchApiClient:
        """Return the client."""
        return self._client

    @override
    async def __aenter__(self) -> Self:
        """Enter an async context.

        Start this background service.

        Returns:
            This background service.
        """
        await super().__aenter__()
        await self._client.__aenter__()
        await self.wait_for_initialization()
        return self

    def new_lifecycle_events_receiver(
        self, dispatch_type: str
    ) -> Receiver[DispatchEvent]:
        """Return new, updated or deleted dispatches receiver.

        Args:
            dispatch_type: The type of the dispatch to listen for.

        Returns:
            A new receiver for new dispatches.
        """
        return self._bg_service.new_lifecycle_events_receiver(dispatch_type)

    async def new_running_state_event_receiver(
        self,
        dispatch_type: str,
        *,
        merge_strategy: MergeStrategy | None = None,
    ) -> Receiver[Dispatch]:
        """Return running state event receiver.

        This receiver will receive a message whenever the current running
        status of a dispatch changes.

        Usually, one message per scheduled run is to be expected.
        However, things get complicated when a dispatch was modified:

        If it was currently running and the modification now says
        it should not be running or running with different parameters,
        then a message will be sent.

        In other words: Any change that is expected to make an actor start, stop
        or adjust itself according to new dispatch options causes a message to be
        sent.

        A non-exhaustive list of possible changes that will cause a message to be sent:
         - The normal scheduled start_time has been reached
         - The duration of the dispatch has been modified
         - The start_time has been modified to be in the future
         - The component selection changed
         - The active status changed
         - The dry_run status changed
         - The payload changed
         - The dispatch was deleted

        `merge_strategy` is an instance of a class derived from
        [`MergeStrategy`][frequenz.dispatch.MergeStrategy] Available strategies
        are:

        * [`MergeByType`][frequenz.dispatch.MergeByType] — merges all dispatches
          of the same type
        * [`MergeByTypeTarget`][frequenz.dispatch.MergeByTypeTarget] — merges all
          dispatches of the same type and target
        * `None` — no merging, just send all events (default)

        Running intervals from multiple dispatches will be merged, according to
        the chosen strategy.

        While merging, stop events are ignored as long as at least one
        merge-criteria-matching dispatch remains active.

        Args:
            dispatch_type: The type of the dispatch to listen for.
            merge_strategy: The type of the strategy to merge running intervals.

        Returns:
            A new receiver for dispatches whose running status changed.
        """
        return await self._bg_service.new_running_state_event_receiver(
            dispatch_type,
            merge_strategy=merge_strategy,
        )
