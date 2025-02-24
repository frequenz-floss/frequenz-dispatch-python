# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for the frequenz.dispatch.actor package."""

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from random import randint
from typing import AsyncIterator, Iterator

import async_solipsism
import pytest
import time_machine
from frequenz.channels import Receiver
from frequenz.client.dispatch.recurrence import Frequency, RecurrenceRule
from frequenz.client.dispatch.test.client import FakeClient, to_create_params
from frequenz.client.dispatch.test.generator import DispatchGenerator
from frequenz.client.dispatch.types import Dispatch as BaseDispatch
from pytest import fixture

from frequenz.dispatch import (
    Created,
    Deleted,
    Dispatch,
    DispatchEvent,
    MergeByType,
    MergeByTypeTarget,
    MergeStrategy,
    Updated,
)
from frequenz.dispatch._bg_service import DispatchScheduler


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


@dataclass(frozen=True)
class TestEnv:
    """Test environment for the service."""

    service: DispatchScheduler
    """The actor under test."""
    lifecycle_events: Receiver[DispatchEvent]
    """The receiver for updated dispatches."""
    running_state_change: Receiver[Dispatch]
    """The receiver for ready dispatches."""
    client: FakeClient
    """The fake client for the actor."""
    microgrid_id: int
    """The microgrid id."""


@fixture
async def test_env() -> AsyncIterator[TestEnv]:
    """Return an actor test environment."""
    microgrid_id = randint(1, 100)
    client = FakeClient()

    service = DispatchScheduler(
        microgrid_id=microgrid_id,
        client=client,
    )

    service.start()
    try:
        yield TestEnv(
            service=service,
            lifecycle_events=service.new_lifecycle_events_receiver("TEST_TYPE"),
            running_state_change=await service.new_running_state_event_receiver(
                "TEST_TYPE", merge_strategy=MergeByType()
            ),
            client=client,
            microgrid_id=microgrid_id,
        )
    finally:
        await service.stop()


@fixture
def generator() -> DispatchGenerator:
    """Return a dispatch generator."""
    return DispatchGenerator()


async def test_new_dispatch_created(
    test_env: TestEnv,
    generator: DispatchGenerator,
) -> None:
    """Test that a new dispatch is created."""
    sample = generator.generate_dispatch()

    await _test_new_dispatch_created(test_env, sample)


def update_dispatch(sample: BaseDispatch, dispatch: BaseDispatch) -> BaseDispatch:
    """Update the sample dispatch with the creation fields from the dispatch.

    Args:
        sample: The sample dispatch to update
        dispatch: The dispatch to update the sample with

    Returns:
        The updated sample dispatch
    """
    return replace(
        sample,
        update_time=dispatch.update_time,
        create_time=dispatch.create_time,
        id=dispatch.id,
    )


async def _test_new_dispatch_created(
    test_env: TestEnv,
    sample: BaseDispatch,
) -> Dispatch:
    """Test that a new dispatch is created.

    Args:
        test_env: The actor environment
        sample: The sample dispatch to create

    Returns:
        The sample dispatch that was created
    """
    sample = replace(sample, type="TEST_TYPE")
    await test_env.client.create(**to_create_params(test_env.microgrid_id, sample))

    dispatch_event = await test_env.lifecycle_events.receive()

    match dispatch_event:
        case Deleted(dispatch) | Updated(dispatch):
            assert False, "Expected a created event"
        case Created(dispatch):
            received = Dispatch(update_dispatch(sample, dispatch))
            assert dispatch == received

    return dispatch


async def test_existing_dispatch_updated(
    test_env: TestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that an existing dispatch is updated."""
    sample = generator.generate_dispatch()
    sample = replace(
        sample,
        active=False,
        recurrence=replace(sample.recurrence, frequency=Frequency.DAILY),
    )

    fake_time.shift(timedelta(seconds=1))

    sample = await _test_new_dispatch_created(test_env, sample)
    fake_time.shift(timedelta(seconds=1))

    updated = await test_env.client.update(
        microgrid_id=test_env.microgrid_id,
        dispatch_id=sample.id,
        new_fields={
            "active": True,
            "recurrence.frequency": Frequency.UNSPECIFIED,
        },
    )
    fake_time.shift(timedelta(seconds=1))

    dispatch_event = await test_env.lifecycle_events.receive()
    match dispatch_event:
        case Created(dispatch) | Deleted(dispatch):
            assert False, f"Expected an updated event, got {dispatch_event}"
        case Updated(dispatch):
            assert dispatch == Dispatch(updated)

    await asyncio.sleep(1)


async def test_existing_dispatch_deleted(
    test_env: TestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that an existing dispatch is deleted."""
    sample = await _test_new_dispatch_created(test_env, generator.generate_dispatch())

    await test_env.client.delete(
        microgrid_id=test_env.microgrid_id, dispatch_id=sample.id
    )
    fake_time.shift(timedelta(seconds=10))
    await asyncio.sleep(10)

    dispatch_event = await test_env.lifecycle_events.receive()
    match dispatch_event:
        case Created(dispatch) | Updated(dispatch):
            assert False, "Expected a deleted event"
        case Deleted(dispatch):
            sample._set_deleted()  # pylint: disable=protected-access
            assert dispatch == sample


async def test_dispatch_inf_duration_deleted(
    test_env: TestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that a dispatch with infinite duration can be deleted while running."""
    # Generate a dispatch with infinite duration (duration=None)
    sample = generator.generate_dispatch()
    sample = replace(
        sample,
        active=True,
        duration=None,
        start_time=_now() + timedelta(seconds=5),
        type="TEST_TYPE",
    )
    # Create the dispatch
    sample = await _test_new_dispatch_created(test_env, sample)
    # Advance time to when the dispatch should start
    fake_time.shift(timedelta(seconds=40))
    await asyncio.sleep(40)
    # Expect notification of the dispatch being ready to run
    ready_dispatch = await test_env.running_state_change.receive()
    assert ready_dispatch.started

    # Now delete the dispatch
    await test_env.client.delete(
        microgrid_id=test_env.microgrid_id, dispatch_id=sample.id
    )
    fake_time.shift(timedelta(seconds=10))
    await asyncio.sleep(1)
    # Expect notification to stop the dispatch
    done_dispatch = await test_env.running_state_change.receive()
    assert done_dispatch.started is False


async def test_dispatch_inf_duration_updated_stopped_started(
    test_env: TestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that a dispatch with infinite duration can be stopped and started by updating it."""
    # Generate a dispatch with infinite duration (duration=None)
    sample = generator.generate_dispatch()
    sample = replace(
        sample,
        active=True,
        duration=None,
        start_time=_now() + timedelta(seconds=5),
        type="TEST_TYPE",
    )
    # Create the dispatch
    sample = await _test_new_dispatch_created(test_env, sample)
    # Advance time to when the dispatch should start
    fake_time.shift(timedelta(seconds=40))
    await asyncio.sleep(40)
    # Expect notification of the dispatch being ready to run
    ready_dispatch = await test_env.running_state_change.receive()
    assert ready_dispatch.started

    # Now update the dispatch to set active=False (stop it)
    await test_env.client.update(
        microgrid_id=test_env.microgrid_id,
        dispatch_id=sample.id,
        new_fields={"active": False},
    )
    fake_time.shift(timedelta(seconds=10))
    await asyncio.sleep(1)
    # Expect notification to stop the dispatch
    stopped_dispatch = await test_env.running_state_change.receive()
    assert stopped_dispatch.started is False

    # Now update the dispatch to set active=True (start it again)
    await test_env.client.update(
        microgrid_id=test_env.microgrid_id,
        dispatch_id=sample.id,
        new_fields={"active": True},
    )
    fake_time.shift(timedelta(seconds=10))
    await asyncio.sleep(1)
    # Expect notification of the dispatch being ready to run again
    started_dispatch = await test_env.running_state_change.receive()
    assert started_dispatch.started


async def test_dispatch_inf_duration_updated_to_finite_and_stops(
    test_env: TestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test updating an inf. duration changing to finite.

    Test that updating an infinite duration dispatch to a finite duration causes
    it to stop if the duration has passed.
    """
    # Generate a dispatch with infinite duration (duration=None)
    sample = generator.generate_dispatch()
    sample = replace(
        sample,
        active=True,
        duration=None,
        start_time=_now() + timedelta(seconds=5),
        type="TEST_TYPE",
    )
    # Create the dispatch
    sample = await _test_new_dispatch_created(test_env, sample)
    # Advance time to when the dispatch should start
    fake_time.shift(timedelta(seconds=10))
    await asyncio.sleep(1)
    # Expect notification of the dispatch being ready to run
    ready_dispatch = await test_env.running_state_change.receive()
    assert ready_dispatch.started

    # Update the dispatch to set duration to a finite duration that has already passed
    # The dispatch has been running for 5 seconds; set duration to 5 seconds
    await test_env.client.update(
        microgrid_id=test_env.microgrid_id,
        dispatch_id=sample.id,
        new_fields={"duration": timedelta(seconds=5)},
    )
    # Advance time to allow the update to be processed
    fake_time.shift(timedelta(seconds=1))
    await asyncio.sleep(1)
    # Expect notification to stop the dispatch because the duration has passed
    stopped_dispatch = await test_env.running_state_change.receive()
    assert stopped_dispatch.started is False


async def test_dispatch_schedule(
    test_env: TestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that a random dispatch is scheduled correctly."""
    sample = replace(
        generator.generate_dispatch(),
        active=True,
        duration=timedelta(seconds=10),
        type="TEST_TYPE",
    )
    await test_env.client.create(**to_create_params(test_env.microgrid_id, sample))
    dispatch = Dispatch(test_env.client.dispatches(test_env.microgrid_id)[0])

    next_run = dispatch.next_run_after(_now())
    assert next_run is not None

    fake_time.shift(next_run - _now() - timedelta(seconds=1))
    await asyncio.sleep(1)

    # Expect notification of the dispatch being ready to run
    ready_dispatch = await test_env.running_state_change.receive()

    assert ready_dispatch == dispatch

    assert dispatch.duration is not None
    # Shift time to the end of the dispatch
    fake_time.shift(dispatch.duration + timedelta(seconds=1))
    await asyncio.sleep(1)

    # Expect notification to stop the dispatch
    done_dispatch = await test_env.running_state_change.receive()
    assert done_dispatch == dispatch

    await asyncio.sleep(1)


async def test_dispatch_inf_duration_updated_to_finite_and_continues(
    test_env: TestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that updating an infinite duration dispatch to a finite duration.

    Test that updating an infinite duration dispatch to a finite
    allows it to continue running if the duration hasn't passed.
    """
    # Generate a dispatch with infinite duration (duration=None)
    sample = generator.generate_dispatch()
    sample = replace(
        sample,
        active=True,
        duration=None,
        start_time=_now() + timedelta(seconds=5),
        type="TEST_TYPE",
    )
    # Create the dispatch
    sample = await _test_new_dispatch_created(test_env, sample)
    # Advance time to when the dispatch should start
    fake_time.shift(timedelta(seconds=10))
    await asyncio.sleep(1)
    # Expect notification of the dispatch being ready to run
    ready_dispatch = await test_env.running_state_change.receive()
    assert ready_dispatch.started

    # Update the dispatch to set duration to a finite duration that hasn't passed yet
    # The dispatch has been running for 5 seconds; set duration to 100 seconds
    await test_env.client.update(
        microgrid_id=test_env.microgrid_id,
        dispatch_id=sample.id,
        new_fields={"duration": timedelta(seconds=100)},
    )
    # Advance time slightly to process the update
    fake_time.shift(timedelta(seconds=1))
    await asyncio.sleep(1)
    # The dispatch should continue running
    # Advance time until the total running time reaches 100 seconds
    fake_time.shift(timedelta(seconds=94))
    await asyncio.sleep(1)
    # Expect notification to stop the dispatch because the duration has now passed
    stopped_dispatch = await test_env.running_state_change.receive()
    assert stopped_dispatch.started is False


async def test_dispatch_new_but_finished(
    test_env: TestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that a finished dispatch is not started at startup."""
    # Generate a dispatch that is already finished
    finished_dispatch = generator.generate_dispatch()
    finished_dispatch = replace(
        finished_dispatch,
        active=True,
        duration=timedelta(seconds=5),
        start_time=_now() - timedelta(seconds=50),
        recurrence=RecurrenceRule(),
        type="TEST_TYPE",
    )
    # Inject an old dispatch
    test_env.client.set_dispatches(test_env.microgrid_id, [finished_dispatch])
    await test_env.service.stop()
    test_env.service.start()
    test_env = replace(
        test_env,
        lifecycle_events=test_env.service.new_lifecycle_events_receiver("TEST_TYPE"),
        running_state_change=(
            await test_env.service.new_running_state_event_receiver(
                "TEST_TYPE", merge_strategy=MergeByType()
            )
        ),
    )

    fake_time.shift(timedelta(seconds=1))
    # Process the lifecycle event caused by the old dispatch at startup
    await test_env.lifecycle_events.receive()

    await asyncio.sleep(1)

    # Create another dispatch the normal way
    new_dispatch = generator.generate_dispatch()
    new_dispatch = replace(
        new_dispatch,
        active=True,
        duration=timedelta(seconds=10),
        start_time=_now() + timedelta(seconds=500),
        recurrence=RecurrenceRule(),
        type="TEST_TYPE",
    )

    new_dispatch = await _test_new_dispatch_created(test_env, new_dispatch)
    assert new_dispatch.started is False

    # Advance time to when the new dispatch should still not start
    fake_time.shift(timedelta(seconds=100))

    assert await test_env.running_state_change.receive() == new_dispatch

    await asyncio.sleep(1)


async def test_notification_on_actor_start(
    test_env: TestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that the actor sends notifications for all running dispatches on start."""
    # Generate a dispatch that is already running
    running_dispatch = generator.generate_dispatch()
    running_dispatch = replace(
        running_dispatch,
        active=True,
        duration=timedelta(seconds=10),
        start_time=_now() - timedelta(seconds=5),
        recurrence=RecurrenceRule(),
        type="TEST_TYPE",
    )
    # Generate a dispatch that is not running
    stopped_dispatch = generator.generate_dispatch()
    stopped_dispatch = replace(
        stopped_dispatch,
        active=False,
        duration=timedelta(seconds=5),
        start_time=_now() - timedelta(seconds=5),
        recurrence=RecurrenceRule(),
        type="TEST_TYPE",
    )
    await test_env.service.stop()

    # Create the dispatches
    test_env.client.set_dispatches(
        test_env.microgrid_id, [running_dispatch, stopped_dispatch]
    )
    test_env.service.start()

    fake_time.shift(timedelta(seconds=1))
    await asyncio.sleep(1)

    # Expect notification of the running dispatch being ready to run
    ready_dispatch = await test_env.running_state_change.receive()
    assert ready_dispatch.started


@pytest.mark.parametrize("merge_strategy", [MergeByType(), MergeByTypeTarget()])
async def test_multiple_dispatches_merge_running_intervals(
    fake_time: time_machine.Coordinates,
    generator: DispatchGenerator,
    merge_strategy: MergeStrategy,
) -> None:
    """Test that multiple dispatches are merged into a single running interval."""
    microgrid_id = randint(1, 100)
    client = FakeClient()
    service = DispatchScheduler(
        microgrid_id=microgrid_id,
        client=client,
    )
    service.start()

    receiver = await service.new_running_state_event_receiver(
        "TEST_TYPE", merge_strategy=merge_strategy
    )

    # Create two overlapping dispatches
    dispatch1 = replace(
        generator.generate_dispatch(),
        active=True,
        duration=timedelta(seconds=30),
        target=[1, 2] if isinstance(merge_strategy, MergeByType) else [3, 4],
        start_time=_now() + timedelta(seconds=5),
        recurrence=RecurrenceRule(),
        type="TEST_TYPE",
    )
    dispatch2 = replace(
        generator.generate_dispatch(),
        active=True,
        duration=timedelta(seconds=10),
        target=[3, 4],
        start_time=_now() + timedelta(seconds=10),  # starts after dispatch1
        recurrence=RecurrenceRule(),
        type="TEST_TYPE",
    )
    lifecycle_events = service.new_lifecycle_events_receiver("TEST_TYPE")

    await client.create(**to_create_params(microgrid_id, dispatch1))
    await client.create(**to_create_params(microgrid_id, dispatch2))

    # Wait for both to be registered
    await lifecycle_events.receive()
    await lifecycle_events.receive()

    # Move time forward to start both dispatches
    fake_time.shift(timedelta(seconds=15))
    await asyncio.sleep(1)

    started1 = await receiver.receive()
    started2 = await receiver.receive()

    assert started1.started
    assert started2.started

    # Stop dispatch2 first, but merge_running_intervals=TYPE means as long as dispatch1 runs,
    # we do not send a stop event
    await client.update(
        microgrid_id=microgrid_id, dispatch_id=started2.id, new_fields={"active": False}
    )
    fake_time.shift(timedelta(seconds=5))
    await asyncio.sleep(1)

    # Now stop dispatch1 as well
    fake_time.shift(timedelta(seconds=15))
    await asyncio.sleep(1)

    # Now we expect a single stop event for the merged window
    stopped = await receiver.receive()
    assert not stopped.started

    await service.stop()


@pytest.mark.parametrize("merge_strategy", [MergeByType(), MergeByTypeTarget()])
async def test_multiple_dispatches_sequential_intervals_merge(
    fake_time: time_machine.Coordinates,
    generator: DispatchGenerator,
    merge_strategy: MergeStrategy,
) -> None:
    """Test that multiple dispatches are merged into a single running interval.

    Even if dispatches don't overlap but are consecutive,
    merge_running_intervals=TPYE should treat them as continuous if any event tries to stop.
    """
    microgrid_id = randint(1, 100)
    client = FakeClient()
    service = DispatchScheduler(microgrid_id=microgrid_id, client=client)
    service.start()

    receiver = await service.new_running_state_event_receiver(
        "TEST_TYPE", merge_strategy=merge_strategy
    )

    dispatch1 = replace(
        generator.generate_dispatch(),
        active=True,
        duration=timedelta(seconds=5),
        # If merging by type, we want to test having different targets in dispatch 1 and 2
        target=[3, 4] if isinstance(merge_strategy, MergeByType) else [1, 2],
        start_time=_now() + timedelta(seconds=5),
        recurrence=RecurrenceRule(),
        type="TEST_TYPE",
    )
    assert dispatch1.duration is not None
    dispatch2 = replace(
        generator.generate_dispatch(),
        active=True,
        duration=timedelta(seconds=5),
        target=[1, 2],
        start_time=dispatch1.start_time + dispatch1.duration,
        recurrence=RecurrenceRule(),
        type="TEST_TYPE",
    )
    lifecycle = service.new_lifecycle_events_receiver("TEST_TYPE")

    await client.create(**to_create_params(microgrid_id, dispatch1))
    await client.create(**to_create_params(microgrid_id, dispatch2))

    # Consume lifecycle events
    await lifecycle.receive()
    await lifecycle.receive()

    fake_time.move_to(dispatch1.start_time + timedelta(seconds=1))
    await asyncio.sleep(1)
    started1 = await receiver.receive()
    assert started1.started

    # Wait for the second dispatch to start
    fake_time.move_to(dispatch2.start_time + timedelta(seconds=1))
    await asyncio.sleep(1)
    started2 = await receiver.receive()
    assert started2.started
    assert started2.target == dispatch2.target

    # Now stop the second dispatch
    assert dispatch2.duration is not None
    fake_time.move_to(dispatch2.start_time + dispatch2.duration + timedelta(seconds=1))
    stopped = await receiver.receive()
    assert not stopped.started


@pytest.mark.parametrize("merge_strategy", [MergeByType(), MergeByTypeTarget()])
async def test_at_least_one_running_filter(
    fake_time: time_machine.Coordinates,
    generator: DispatchGenerator,
    merge_strategy: MergeStrategy,
) -> None:
    """Test scenarios directly tied to the _at_least_one_running logic."""
    microgrid_id = randint(1, 100)
    client = FakeClient()
    service = DispatchScheduler(microgrid_id=microgrid_id, client=client)
    service.start()

    # merge_running_intervals is TYPE, so we use merged intervals
    receiver = await service.new_running_state_event_receiver(
        "TEST_TYPE", merge_strategy=merge_strategy
    )

    # Single dispatch that starts and stops normally
    dispatch = replace(
        generator.generate_dispatch(),
        active=True,
        duration=timedelta(seconds=10),
        target=[1, 2] if isinstance(merge_strategy, MergeByType) else [3, 4],
        start_time=_now() + timedelta(seconds=5),
        recurrence=RecurrenceRule(),
        type="TEST_TYPE",
    )
    _ = merge_strategy.identity(Dispatch(dispatch))

    lifecycle = service.new_lifecycle_events_receiver("TEST_TYPE")
    await client.create(**to_create_params(microgrid_id, dispatch))
    await lifecycle.receive()

    # Move time so it starts
    fake_time.shift(timedelta(seconds=6))
    await asyncio.sleep(1)
    started = await receiver.receive()
    assert started.started

    # Now stop it
    await client.update(
        microgrid_id=microgrid_id, dispatch_id=started.id, new_fields={"active": False}
    )
    fake_time.shift(timedelta(seconds=2))
    await asyncio.sleep(1)
    stopped = await receiver.receive()
    assert not stopped.started

    # Now test scenario with multiple dispatches: one never starts, one starts and stops
    dispatch_a = replace(
        generator.generate_dispatch(),
        active=False,
        duration=timedelta(seconds=10),
        target=[3, 4],
        start_time=_now() + timedelta(seconds=50),
        recurrence=RecurrenceRule(),
        type="TEST_TYPE",
    )
    dispatch_b = replace(
        generator.generate_dispatch(),
        active=True,
        duration=timedelta(seconds=5),
        start_time=_now() + timedelta(seconds=5),
        recurrence=RecurrenceRule(),
        type="TEST_TYPE",
    )
    await client.create(**to_create_params(microgrid_id, dispatch_a))
    await client.create(**to_create_params(microgrid_id, dispatch_b))
    lifecycle = service.new_lifecycle_events_receiver("TEST_TYPE")
    await lifecycle.receive()
    await lifecycle.receive()

    fake_time.shift(timedelta(seconds=6))
    await asyncio.sleep(1)
    started_b = await receiver.receive()
    assert started_b.started

    # Stop dispatch_b before dispatch_a ever becomes active
    await client.update(
        microgrid_id=microgrid_id,
        dispatch_id=started_b.id,
        new_fields={"active": False},
    )
    fake_time.shift(timedelta(seconds=2))
    await asyncio.sleep(1)
    stopped_b = await receiver.receive()
    assert not stopped_b.started
