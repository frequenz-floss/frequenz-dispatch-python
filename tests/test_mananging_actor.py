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
from frequenz.client.dispatch import recurrence
from frequenz.client.dispatch.recurrence import Frequency
from frequenz.client.dispatch.test.client import FakeClient
from frequenz.client.dispatch.test.generator import DispatchGenerator
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


@dataclass
class TestEnv:
    """Test environment."""

    actors_service: ActorDispatcher
    running_status_sender: Sender[Dispatch]
    generator: DispatchGenerator = DispatchGenerator()

    def actor(self, identity: int) -> MockActor:
        """Return the actor."""
        # pylint: disable=protected-access
        assert identity in self.actors_service._actors
        return cast(MockActor, self.actors_service._actors[identity])
        # pylint: enable=protected-access


@fixture
async def test_env() -> AsyncIterator[TestEnv]:
    """Create a test environment."""
    channel = Broadcast[Dispatch](name="dispatch ready test channel")

    actors_service = ActorDispatcher(
        actor_factory=MockActor.create,
        running_status_receiver=channel.new_receiver(),
        dispatch_identity=lambda dispatch: dispatch.id,
    )

    actors_service.start()
    await asyncio.sleep(1)

    yield TestEnv(
        actors_service=actors_service,
        running_status_sender=channel.new_sender(),
    )

    await actors_service.stop()


async def test_simple_start_stop(
    test_env: TestEnv,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test behavior when receiving start/stop messages."""
    now = _now()
    duration = timedelta(minutes=10)
    dispatch = test_env.generator.generate_dispatch()
    dispatch = replace(
        dispatch,
        id=1,
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
    await asyncio.sleep(1)
    await asyncio.sleep(1)

    event = test_env.actor(1).initial_dispatch
    assert event.options == {"test": True}
    assert event.components == dispatch.target
    assert event.dry_run is False

    assert test_env.actor(1).is_running is True

    fake_time.shift(duration)
    await test_env.running_status_sender.send(Dispatch(dispatch))

    # Give await actor.stop a chance to run
    await asyncio.sleep(1)

    # pylint: disable=protected-access
    assert 1 not in test_env.actors_service._actors
    # pylint: enable=protected-access


def test_heapq_dispatch_compare(test_env: TestEnv) -> None:
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


def test_heapq_dispatch_start_stop_compare(test_env: TestEnv) -> None:
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


async def test_dry_run(test_env: TestEnv, fake_time: time_machine.Coordinates) -> None:
    """Test the dry run mode."""
    dispatch = test_env.generator.generate_dispatch()
    dispatch = replace(
        dispatch,
        id=1,
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
    identity: Callable[[Dispatch], int] = (
        strategy.identity if strategy else lambda dispatch: dispatch.id
    )

    class MyFakeClient(FakeClient):
        """Fake client for testing."""

        def __init__(self, *, server_url: str, key: str):
            assert server_url
            assert key
            super().__init__()

    mid = 1

    # Patch `Client` class in Dispatcher with MyFakeClient
    with patch("frequenz.dispatch._dispatcher.Client", MyFakeClient):
        dispatcher = Dispatcher(
            microgrid_id=mid, server_url="grpc://test-url", key="test-key"
        )
        dispatcher.start()

        channel = Broadcast[Dispatch](name="dispatch ready test channel")
        sender = channel.new_sender()

        async def new_mock_receiver(
            _: Dispatcher, dispatch_type: str, *, merge_strategy: MergeStrategy | None
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
