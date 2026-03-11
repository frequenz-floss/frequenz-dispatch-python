# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals

"""Tests for scheduler edge cases in DispatchScheduler."""

import asyncio
from collections.abc import AsyncIterator, Iterator
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from random import randint

import async_solipsism
import time_machine
from frequenz.channels import Receiver
from frequenz.client.common.microgrid import MicrogridId
from frequenz.client.dispatch.recurrence import RecurrenceRule
from frequenz.client.dispatch.test.client import FakeClient, to_create_params
from frequenz.client.dispatch.test.generator import DispatchGenerator
from pytest import fixture

from frequenz.dispatch import Dispatch, DispatchEvent
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
    with time_machine.travel(destination=0, tick=False) as traveller:
        yield traveller


def _now() -> datetime:
    """Return the current time in UTC."""
    return datetime.now(tz=timezone.utc)


@fixture
def microgrid_id() -> MicrogridId:
    """Return a random microgrid ID."""
    return MicrogridId(randint(1, 100))


@fixture
def client() -> FakeClient:
    """Return a fake dispatch API client."""
    return FakeClient()


@fixture
async def scheduler(
    microgrid_id: MicrogridId, client: FakeClient
) -> AsyncIterator[DispatchScheduler]:
    """Start a DispatchScheduler and stop it after the test."""
    sched = DispatchScheduler(microgrid_id=microgrid_id, client=client)
    sched.start()
    try:
        yield sched
    finally:
        await sched.stop()


@fixture
async def event_receiver(scheduler: DispatchScheduler) -> Receiver[Dispatch]:
    """Return a running-state event receiver for SET_POWER dispatches."""
    return await scheduler.new_running_state_event_receiver(
        "SET_POWER", merge_strategy=None
    )


@fixture
def lifecycle_receiver(scheduler: DispatchScheduler) -> Receiver[DispatchEvent]:
    """Return a lifecycle event receiver for SET_POWER dispatches."""
    return scheduler.new_lifecycle_events_receiver("SET_POWER")


async def test_parallel_dispatches_same_window_and_follow_up_window(
    fake_time: time_machine.Coordinates,
    generator: DispatchGenerator,
    microgrid_id: MicrogridId,
    client: FakeClient,
    event_receiver: Receiver[Dispatch],
    lifecycle_receiver: Receiver[DispatchEvent],
) -> None:
    """Test that unmerged dispatches stop correctly at a shared boundary.

    This mirrors the SET_POWER edge case where multiple dispatches share the
    exact same start time and duration, and another batch starts exactly when
    the first batch ends.

    This is an opportunistic test: it tries to detect corruption in the
    _scheduled_events heap by exercising the production traffic shape around
    shared boundaries. The broken heap ordering depends on runtime queue
    layouts that this harness does not reproduce reliably, but the test still
    covers the correct boundary behaviour.
    """
    start_time = _now() + timedelta(seconds=5)
    duration = timedelta(seconds=15)
    follow_up_start_time = start_time + duration

    first_batch = [
        replace(
            generator.generate_dispatch(),
            active=True,
            duration=duration,
            start_time=start_time,
            recurrence=RecurrenceRule(),
            type="SET_POWER",
        )
        for _ in range(3)
    ]
    second_batch = [
        replace(
            generator.generate_dispatch(),
            active=True,
            duration=duration,
            start_time=follow_up_start_time,
            recurrence=RecurrenceRule(),
            type="SET_POWER",
        )
        for _ in range(4)
    ]

    for dispatch in [*first_batch, *second_batch]:
        await client.create(**to_create_params(microgrid_id, dispatch))

    for _ in range(7):
        await lifecycle_receiver.receive()

    fake_time.move_to(start_time + timedelta(seconds=1))
    await asyncio.sleep(1)

    first_start_events = [await event_receiver.receive() for _ in range(3)]
    assert {dispatch.id for dispatch in first_start_events} == {
        dispatch.id for dispatch in first_batch
    }
    assert all(dispatch.started for dispatch in first_start_events)

    fake_time.move_to(follow_up_start_time + timedelta(seconds=1))
    await asyncio.sleep(1)

    boundary_events = [await event_receiver.receive() for _ in range(7)]
    second_starts = [dispatch for dispatch in boundary_events if dispatch.started]
    first_stops = [dispatch for dispatch in boundary_events if not dispatch.started]

    assert {dispatch.id for dispatch in second_starts} == {
        dispatch.id for dispatch in second_batch
    }
    assert all(dispatch.started for dispatch in second_starts)

    assert {dispatch.id for dispatch in first_stops} == {
        dispatch.id for dispatch in first_batch
    }
    assert all(not dispatch.started for dispatch in first_stops)

    fake_time.move_to(follow_up_start_time + duration + timedelta(seconds=1))
    await asyncio.sleep(1)

    second_stop_events = [await event_receiver.receive() for _ in range(4)]
    assert {dispatch.id for dispatch in second_stop_events} == {
        dispatch.id for dispatch in second_batch
    }
    assert all(not dispatch.started for dispatch in second_stop_events)


async def test_parallel_dispatches_with_payload_updates_before_start(
    fake_time: time_machine.Coordinates,
    generator: DispatchGenerator,
    microgrid_id: MicrogridId,
    client: FakeClient,
    event_receiver: Receiver[Dispatch],
    lifecycle_receiver: Receiver[DispatchEvent],
) -> None:
    """Test dispatches updated before start still stop correctly.

    Mirrors the production SET_POWER scenario: multiple dispatches with the same
    start time are created and then updated with new power values at different
    times before the start. Updates trigger _remove_scheduled + re-schedule
    which can corrupt the heap, potentially causing stop events to be lost.

    This is an opportunistic test: it tries to detect corruption in the
    _scheduled_events heap by exercising the update-before-start pattern. The
    buggy heap state is timing- and layout-dependent and cannot be forced
    deterministically here, but the test still covers the correct behaviour.
    """
    start_time = _now() + timedelta(minutes=25)
    duration = timedelta(minutes=15)

    updates = [
        (-80000.0, -88740.0, timedelta(seconds=20)),
        (-80000.0, -88740.0, timedelta(minutes=4)),
        (-100000.0, -133680.0, timedelta(seconds=50)),
    ]
    dispatches = []

    for initial_power, updated_power, wait_time in updates:
        dispatch = replace(
            generator.generate_dispatch(),
            active=True,
            duration=duration,
            start_time=start_time,
            recurrence=RecurrenceRule(),
            type="SET_POWER",
            payload={"target_power_w": initial_power},
        )
        dispatches.append(dispatch)
        await client.create(**to_create_params(microgrid_id, dispatch))
        await lifecycle_receiver.receive()
        fake_time.shift(wait_time)
        await asyncio.sleep(1)
        await client.update(
            microgrid_id=microgrid_id,
            dispatch_id=dispatch.id,
            new_fields={"payload": {"target_power_w": updated_power}},
        )
        fake_time.shift(timedelta(seconds=1))
        await asyncio.sleep(1)
        await lifecycle_receiver.receive()

    fake_time.move_to(start_time + timedelta(seconds=1))
    await asyncio.sleep(1)

    start_events = [await event_receiver.receive() for _ in dispatches]
    assert all(dispatch.started for dispatch in start_events)

    fake_time.move_to(start_time + duration + timedelta(seconds=1))
    await asyncio.sleep(1)

    stop_events = [await event_receiver.receive() for _ in dispatches]
    assert all(not dispatch.started for dispatch in stop_events)
    assert {dispatch.id for dispatch in stop_events} == {
        dispatch.id for dispatch in start_events
    }


async def test_dispatch_payload_update_at_stop_boundary(
    fake_time: time_machine.Coordinates,
    generator: DispatchGenerator,
    microgrid_id: MicrogridId,
    client: FakeClient,
    event_receiver: Receiver[Dispatch],
    lifecycle_receiver: Receiver[DispatchEvent],
) -> None:
    """Test that an update at the stop boundary still stops the actor.

    An update exactly at the stop boundary can remove the pending stop event
    before the timer fires it. Without an explicit notification at that point,
    the actor is left running even though the dispatch window has elapsed.

    In practice this test is still opportunistic: a payload change triggers an
    immediate update notification in this harness, so it also passes without the
    fix. It still covers the boundary-update shape from production.
    """
    start_time = _now() + timedelta(seconds=5)
    duration = timedelta(seconds=15)

    dispatch = replace(
        generator.generate_dispatch(),
        active=True,
        duration=duration,
        start_time=start_time,
        recurrence=RecurrenceRule(),
        type="SET_POWER",
        payload={"target_power_w": -88740.0},
    )
    await client.create(**to_create_params(microgrid_id, dispatch))
    await lifecycle_receiver.receive()

    fake_time.move_to(start_time + timedelta(seconds=1))
    await asyncio.sleep(1)

    started = await event_receiver.receive()
    assert started.started

    fake_time.move_to(start_time + duration)
    await asyncio.sleep(0)

    await client.update(
        microgrid_id=microgrid_id,
        dispatch_id=dispatch.id,
        new_fields={"payload": {"target_power_w": -99999.0}},
    )
    fake_time.shift(timedelta(seconds=1))
    await asyncio.sleep(1)

    stop_event = await event_receiver.receive()
    assert not stop_event.started
