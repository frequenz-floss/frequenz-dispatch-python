# LICENSE: ALL RIGHTS RESERVED
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Test the dispatch runner."""

import asyncio
import heapq
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Callable, Iterator, cast
from unittest.mock import patch

import async_solipsism
import pytest
import time_machine
from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.client.common.microgrid import MicrogridId
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.dispatch import recurrence
from frequenz.client.dispatch.recurrence import Frequency, RecurrenceRule
from frequenz.client.dispatch.test.client import FakeClient
from frequenz.client.dispatch.test.generator import DispatchGenerator
from frequenz.client.dispatch.types import DispatchId, TargetIds
from frequenz.sdk.actor import Actor
from pytest import fixture

from frequenz.dispatch import (
    ActorDispatcher,
    Dispatch,
    Dispatcher,
    DispatchInfo,
    MergeByType,
    MergeByTypeTarget,
    MergeStrategy,
)
from frequenz.dispatch._actor_dispatcher import DispatchActorId
from frequenz.dispatch._bg_service import DispatchScheduler


@fixture
def generator() -> DispatchGenerator:
    """Return a dispatch generator."""
    return DispatchGenerator()


@fixture
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Set the event loop policy to use async_solipsism."""
    policy = async_solipsism.EventLoopPolicy()
    asyncio.set_event_loop_policy(policy)
    return policy


@fixture
def fake_time() -> Iterator[time_machine.Coordinates]:
    """Replace real time with a time machine that doesn't automatically tick."""
    # destination can be a datetime or a timestamp (int), so are moving to the
    # epoch (in UTC!)
    with time_machine.travel(destination=0, tick=False) as traveller:
        yield traveller


def _now() -> datetime:
    """Return the current time in UTC."""
    return datetime.now(tz=timezone.utc)


class MockActor(Actor):
    """Mock actor for testing."""

    def __init__(
        self, initial_dispatch: DispatchInfo, receiver: Receiver[DispatchInfo]
    ) -> None:
        """Initialize the actor."""
        super().__init__(name="MockActor")
        self.initial_dispatch = initial_dispatch
        self.receiver = receiver

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(1)

    @classmethod
    async def create(
        cls, initial_dispatch: DispatchInfo, receiver: Receiver[DispatchInfo]
    ) -> "MockActor":
        """Create a new actor."""
        actor = cls(initial_dispatch, receiver)
        return actor

    @classmethod
    async def create_fail(
        cls, __: DispatchInfo, _: Receiver[DispatchInfo]
    ) -> "MockActor":
        """Create a new actor."""
        raise ValueError("Failed to create actor")


@dataclass
class _TestEnv:
    """Test environment."""

    actors_service: ActorDispatcher
    running_status_sender: Sender[Dispatch]
    generator: DispatchGenerator = DispatchGenerator()

    def actor(self, identity: DispatchActorId | int) -> MockActor:
        """Return the actor."""
        if isinstance(identity, int):
            identity = DispatchActorId(identity)

        # pylint: disable=protected-access
        assert identity in self.actors_service._actors
        return cast(MockActor, self.actors_service._actors[identity].actor)
        # pylint: enable=protected-access

    def is_running(self, identity: int) -> bool:
        """Return whether the actor is running."""
        # pylint: disable-next=protected-access
        if DispatchActorId(identity) not in self.actors_service._actors:
            return False

        return self.actor(identity).is_running


@fixture
async def test_env() -> AsyncIterator[_TestEnv]:
    """Create a test environment."""
    channel = Broadcast[Dispatch](name="dispatch ready test channel")

    actors_service = ActorDispatcher(
        actor_factory=MockActor.create,
        running_status_receiver=channel.new_receiver(),
        dispatch_identity=lambda dispatch: DispatchActorId(dispatch.id),
    )

    actors_service.start()
    await asyncio.sleep(1)

    yield _TestEnv(
        actors_service=actors_service,
        running_status_sender=channel.new_sender(),
    )

    await actors_service.stop()


async def test_simple_start_stop(
    test_env: _TestEnv,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test behavior when receiving start/stop messages."""
    now = _now()
    duration = timedelta(minutes=10)
    dispatch = test_env.generator.generate_dispatch()
    dispatch = replace(
        dispatch,
        id=DispatchId(1),
        active=True,
        dry_run=False,
        duration=duration,
        target=TargetIds(1, 10, 15),
        start_time=now,
        payload={"test": True},
        type="UNIT_TEST",
        recurrence=replace(
            dispatch.recurrence,
            frequency=Frequency.UNSPECIFIED,
        ),
    )

    # Send status update to start actor, expect no DispatchInfo for the start
    await test_env.running_status_sender.send(Dispatch(dispatch))
    fake_time.shift(timedelta(seconds=1))
    await asyncio.sleep(1)
    await asyncio.sleep(1)

    event = test_env.actor(1).initial_dispatch
    assert event.options == {"test": True}
    assert event.components == TargetIds(
        ComponentId(1), ComponentId(10), ComponentId(15)
    )
    assert event.dry_run is False

    assert test_env.actor(1).is_running is True

    fake_time.shift(duration)
    await test_env.running_status_sender.send(Dispatch(dispatch))

    # Give await actor.stop a chance to run
    await asyncio.sleep(1)

    # pylint: disable=protected-access
    assert 1 not in test_env.actors_service._actors
    # pylint: enable=protected-access


async def test_start_failed(
    test_env: _TestEnv, fake_time: time_machine.Coordinates
) -> None:
    """Test auto-retry after 60 seconds."""
    # pylint: disable=protected-access
    test_env.actors_service._actor_factory = MockActor.create_fail

    now = _now()
    duration = timedelta(minutes=10)
    dispatch = test_env.generator.generate_dispatch()
    dispatch = replace(
        dispatch,
        id=DispatchId(1),
        active=True,
        dry_run=False,
        duration=duration,
        start_time=now,
        payload={"test": True},
        type="UNIT_TEST",
        recurrence=replace(
            dispatch.recurrence,
            frequency=Frequency.UNSPECIFIED,
        ),
    )

    # Send status update to start actor, expect no DispatchInfo for the start
    await test_env.running_status_sender.send(Dispatch(dispatch))
    fake_time.shift(timedelta(seconds=1))

    # Replace failing mock actor factory with a working one
    test_env.actors_service._actor_factory = MockActor.create

    # Give retry task time to start
    await asyncio.sleep(1)

    fake_time.shift(timedelta(seconds=65))
    await asyncio.sleep(65)

    assert test_env.actor(1).is_running is True


def test_heapq_dispatch_compare(test_env: _TestEnv) -> None:
    """Test that the heapq compare function works."""
    dispatch1 = test_env.generator.generate_dispatch()
    dispatch2 = test_env.generator.generate_dispatch()

    # Simulate two dispatches with the same 'until' time
    now = datetime.now(timezone.utc)
    until_time = now + timedelta(minutes=5)

    # Create the heap
    scheduled_events: list[DispatchScheduler.QueueItem] = []

    # Push two events with the same 'until' time onto the heap
    heapq.heappush(
        scheduled_events,
        DispatchScheduler.QueueItem(until_time, Dispatch(dispatch1), True),
    )
    heapq.heappush(
        scheduled_events,
        DispatchScheduler.QueueItem(until_time, Dispatch(dispatch2), True),
    )


def test_heapq_dispatch_start_stop_compare(test_env: _TestEnv) -> None:
    """Test that the heapq compare function works."""
    dispatch1 = test_env.generator.generate_dispatch()
    dispatch2 = test_env.generator.generate_dispatch()

    # Simulate two dispatches with the same 'until' time
    now = datetime.now(timezone.utc)
    until_time = now + timedelta(minutes=5)

    # Create the heap
    scheduled_events: list[DispatchScheduler.QueueItem] = []

    # Push two events with the same 'until' time onto the heap
    heapq.heappush(
        scheduled_events,
        DispatchScheduler.QueueItem(until_time, Dispatch(dispatch1), stop_event=False),
    )
    heapq.heappush(
        scheduled_events,
        DispatchScheduler.QueueItem(until_time, Dispatch(dispatch2), stop_event=True),
    )

    assert scheduled_events[0].dispatch_id == dispatch1.id
    assert scheduled_events[1].dispatch_id == dispatch2.id


async def test_dry_run(test_env: _TestEnv, fake_time: time_machine.Coordinates) -> None:
    """Test the dry run mode."""
    dispatch = test_env.generator.generate_dispatch()
    dispatch = replace(
        dispatch,
        id=DispatchId(1),
        dry_run=True,
        active=True,
        start_time=_now(),
        duration=timedelta(minutes=10),
        type="UNIT_TEST",
        recurrence=replace(
            dispatch.recurrence,
            frequency=Frequency.UNSPECIFIED,
        ),
    )

    await test_env.running_status_sender.send(Dispatch(dispatch))
    fake_time.shift(timedelta(seconds=1))
    await asyncio.sleep(1)

    event = test_env.actor(1).initial_dispatch

    assert event.dry_run is dispatch.dry_run
    assert event.components == dispatch.target
    assert event.options == dispatch.payload
    assert test_env.actor(1).is_running is True

    assert dispatch.duration is not None
    fake_time.shift(dispatch.duration)
    await test_env.running_status_sender.send(Dispatch(dispatch))

    # Give await actor.stop a chance to run
    await asyncio.sleep(1)


@pytest.mark.parametrize("strategy", [MergeByTypeTarget(), MergeByType(), None])
async def test_manage_abstraction(
    fake_time: time_machine.Coordinates,
    generator: DispatchGenerator,
    strategy: MergeStrategy | None,
) -> None:
    """Test Dispatcher.start_managing sets up correctly."""
    identity: Callable[[Dispatch], DispatchActorId] = (
        strategy.identity if strategy else lambda dispatch: DispatchActorId(dispatch.id)
    )

    class MyFakeClient(FakeClient):
        """Fake client for testing."""

        def __init__(
            self,
            *,
            server_url: str,
            key: str,
            call_timeout: timedelta,
            stream_timeout: timedelta,
        ) -> None:
            assert server_url
            assert key
            assert call_timeout
            assert stream_timeout
            super().__init__()

    mid = MicrogridId(1)

    # Patch `Client` class in Dispatcher with MyFakeClient
    with patch("frequenz.dispatch._dispatcher.DispatchApiClient", MyFakeClient):
        dispatcher = Dispatcher(
            microgrid_id=mid, server_url="grpc://test-url", key="test-key"
        )
        dispatcher.start()

        channel = Broadcast[Dispatch](name="dispatch ready test channel")
        sender = channel.new_sender()

        async def new_mock_receiver(
            _: Dispatcher,
            dispatch_type: str,
            *,
            merge_strategy: MergeStrategy | None,
        ) -> Receiver[Dispatch]:
            assert dispatch_type == "MANAGE_TEST"
            assert merge_strategy is strategy
            return channel.new_receiver()

        with patch(
            "frequenz.dispatch._dispatcher.Dispatcher.new_running_state_event_receiver",
            new_mock_receiver,
        ):
            await dispatcher.start_managing(
                dispatch_type="MANAGE_TEST",
                actor_factory=MockActor.create,
                merge_strategy=strategy,
            )

        # pylint: disable=protected-access
        assert "MANAGE_TEST" in dispatcher._actor_dispatchers
        actor_manager = dispatcher._actor_dispatchers["MANAGE_TEST"]
        # pylint: disable=comparison-with-callable
        assert actor_manager._actor_factory == MockActor.create
        # pylint: enable=comparison-with-callable

        dispatch = Dispatch(
            replace(
                generator.generate_dispatch(),
                start_time=_now(),
                duration=timedelta(minutes=10),
                recurrence=recurrence.RecurrenceRule(),
                active=True,
                type="MANAGE_TEST",
            )
        )

        fake_time.move_to(dispatch.start_time + timedelta(seconds=1))
        assert dispatch.started

        # Send a dispatch to start an actor instance
        await sender.send(dispatch)

        # Give the actor a chance to start
        await asyncio.sleep(1)

        # Check if actor instance is created
        assert identity(dispatch) in actor_manager._actors


async def test_actor_dispatcher_update_isolation(
    test_env: _TestEnv,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that updates for one dispatch don't affect other actors of the same type."""
    dispatch_type = "ISOLATION_TEST"
    start_time = _now()
    duration = timedelta(minutes=5)

    # Create first dispatch
    dispatch1_spec = replace(
        test_env.generator.generate_dispatch(),
        id=DispatchId(101),  # Unique ID
        type=dispatch_type,
        active=True,
        dry_run=False,
        start_time=start_time + timedelta(seconds=1),  # Stagger start slightly
        duration=duration,
        payload={"instance": 1},
        recurrence=RecurrenceRule(),
    )
    dispatch1 = Dispatch(dispatch1_spec)

    # Create second dispatch of the same type, different ID
    dispatch2_spec = replace(
        test_env.generator.generate_dispatch(),
        id=DispatchId(102),  # Unique ID
        type=dispatch_type,  # Same type
        active=True,
        dry_run=False,
        start_time=start_time + timedelta(seconds=2),  # Stagger start slightly
        duration=duration,
        payload={"instance": 2},
        recurrence=RecurrenceRule(),
    )
    dispatch2 = Dispatch(dispatch2_spec)

    # Send dispatch 1 to start actor 1
    # print(f"Sending dispatch 1: {dispatch1}")
    await test_env.running_status_sender.send(dispatch1)
    fake_time.shift(timedelta(seconds=1.1))  # Move time past dispatch1 start
    await asyncio.sleep(0.1)  # Allow actor to start

    assert test_env.is_running(101), "Actor 1 should be running"
    actor1 = test_env.actor(101)
    assert actor1 is not None
    # pylint: disable-next=protected-access
    assert actor1.initial_dispatch._src.id == DispatchId(101)
    assert actor1.initial_dispatch.options == {"instance": 1}
    assert not test_env.is_running(102), "Actor 2 should not be running yet"

    # Send dispatch 2 to start actor 2
    # print(f"Sending dispatch 2: {dispatch2}")
    await test_env.running_status_sender.send(dispatch2)
    fake_time.shift(timedelta(seconds=1))  # Move time past dispatch2 start
    await asyncio.sleep(0.1)  # Allow actor to start

    assert test_env.actor(101).is_running, "Actor 1 should still be running"
    assert test_env.actor(102).is_running, "Actor 2 should now be running"
    actor2 = test_env.actor(102)
    assert actor2 is not None
    # pylint: disable-next=protected-access
    assert actor2.initial_dispatch._src.id == DispatchId(102)
    assert actor2.initial_dispatch.options == {"instance": 2}

    # Now, send an update to stop dispatch 1
    dispatch1_stop = Dispatch(
        replace(dispatch1_spec, duration=timedelta(seconds=1), active=False)
    )
    # print(f"Sending stop for dispatch 1: {dispatch1_stop}")
    await test_env.running_status_sender.send(dispatch1_stop)
    await asyncio.sleep(0.1)  # Allow ActorDispatcher to process the stop

    # THE CORE ASSERTION: Actor 1 should stop, Actor 2 should remain running
    # pylint: disable=protected-access
    assert (
        101 not in test_env.actors_service._actors
    ), "Actor 1 should have been removed"
    # pylint: enable=protected-access
    assert (
        test_env.actor(102).is_running is True
    ), "Actor 2 should be running after Actor 1 stopped"
    # Double check actor1 object state if needed (though removal is stronger check)
    # assert not actor1.is_running

    # Cleanup: Stop Actor 2
    dispatch2_stop = Dispatch(replace(dispatch2_spec, active=False))
    # print(f"Sending stop for dispatch 2: {dispatch2_stop}")
    await test_env.running_status_sender.send(dispatch2_stop)
    await asyncio.sleep(0.1)  # Allow ActorDispatcher to process the stop

    # pylint: disable=protected-access
    assert (
        102 not in test_env.actors_service._actors
    ), "Actor 2 should have been removed"
    # pylint: enable=protected-access
    assert not test_env.is_running(102), "Actor 2 should be stopped"
