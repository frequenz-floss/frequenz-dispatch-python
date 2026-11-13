# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Deterministic unit tests for ActorDispatcher's continuation handovers.

These drive `ActorDispatcher._handle_dispatch` directly (bypassing its
`running_status_receiver` channel and `_run()` loop), with a plain dict as
the `dispatches_snapshot` and a frozen clock (`fake_time`) so a dispatch's
boundary can be matched exactly without a real-time race. Each test runs in
milliseconds. See `test_continuation_realclock.py` for one end-to-end test
on top of the real `DispatchScheduler`.
"""

import asyncio
import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Iterator, cast

import pytest
import time_machine
from frequenz.channels import Broadcast, Receiver
from frequenz.client.dispatch.recurrence import RecurrenceRule
from frequenz.client.dispatch.test.generator import DispatchGenerator
from frequenz.client.dispatch.types import DispatchId, TargetIds
from frequenz.sdk.actor import Actor
from pytest import fixture

from frequenz.dispatch import (
    ActorDispatcher,
    Dispatch,
    DispatchInfo,
    is_exact_continuation,
)
from frequenz.dispatch._actor_dispatcher import DispatchActorId


@fixture
def generator() -> DispatchGenerator:
    """Return a dispatch generator."""
    return DispatchGenerator()


@fixture
def fake_time() -> Iterator[time_machine.Traveller]:
    """Replace real time with a time machine that doesn't automatically tick."""
    with time_machine.travel(destination=0, tick=False) as traveller:
        yield traveller


def _now() -> datetime:
    """Return the current (possibly frozen) time in UTC."""
    return datetime.now(tz=timezone.utc)


_HANDOVER_EVENTS: list[str] = []


def _continuation_dispatch(
    generator: DispatchGenerator,  # pylint: disable=redefined-outer-name
    *,
    dispatch_id: int,
    start: datetime,
    duration: timedelta | None = timedelta(hours=1),
    active: bool = True,
) -> Dispatch:
    """Create a dispatch for continuation handover tests."""
    return Dispatch(
        replace(
            generator.generate_dispatch(),
            id=DispatchId(dispatch_id),
            type="CONTINUATION_TEST",
            active=active,
            dry_run=False,
            start_time=start,
            duration=duration,
            target=TargetIds(1),
            recurrence=RecurrenceRule(),
        )
    )


def _stopped(
    generator: DispatchGenerator, dispatch: Dispatch
) -> Dispatch:  # pylint: disable=redefined-outer-name
    """Build a deactivated copy of `dispatch`, to send as its stop event.

    `Dispatch` has a custom `__init__` (it takes the whole client dispatch
    as one positional argument), so plain `dataclasses.replace()` doesn't
    work on it directly.
    """
    return _continuation_dispatch(
        generator,
        dispatch_id=int(dispatch.id),
        start=dispatch.start_time,
        duration=dispatch.duration,
        active=False,
    )


class _HandoverActor(Actor):
    """Record start/stop order. Not `ContinuationCapable`."""

    def __init__(self, info: DispatchInfo, receiver: Receiver[DispatchInfo]) -> None:
        """Initialize the actor and record its start."""
        super().__init__(name=f"actor-{int(info.dispatch.id)}")
        self.receiver = receiver
        self.current_dispatch_id = info.dispatch.id
        _HANDOVER_EVENTS.append(f"start {self.name}")

    async def _run(self) -> None:
        async for _ in self.receiver:
            pass

    async def stop(self, msg: str | None = None) -> None:
        """Stop, recording that it happened."""
        _HANDOVER_EVENTS.append(f"stop {self.name}")
        await super().stop(msg)

    @classmethod
    async def create(
        cls, info: DispatchInfo, receiver: Receiver[DispatchInfo]
    ) -> "_HandoverActor":
        """Create a new actor."""
        return cls(info, receiver)


class _RefusingHandoverActor(_HandoverActor):
    """Explicitly refuses every continuation."""

    async def accept_continuation(self, _update: DispatchInfo) -> bool:
        """Refuse every continuation."""
        return False


class _AcceptingHandoverActor(_HandoverActor):
    """Accepts a continuation that matches its current dispatch."""

    def __init__(self, info: DispatchInfo, receiver: Receiver[DispatchInfo]) -> None:
        """Initialize the actor and its acceptance counter."""
        super().__init__(info, receiver)
        self.accept_continuation_calls = 0

    async def accept_continuation(self, update: DispatchInfo) -> bool:
        """Accept a continuation of our own current dispatch."""
        self.accept_continuation_calls += 1
        if update.continuation_of != self.current_dispatch_id:
            return False
        self.current_dispatch_id = update.dispatch.id
        return True


class _TimingOutHandoverActor(_HandoverActor):
    """Never responds to a continuation."""

    async def accept_continuation(self, _update: DispatchInfo) -> bool:
        """Hang well past any reasonable `continuation_accept_timeout`."""
        await asyncio.sleep(1_000_000)
        return True  # pragma: no cover -- never reached


class _RaisingHandoverActor(_HandoverActor):
    """Raises instead of answering a continuation."""

    async def accept_continuation(self, _update: DispatchInfo) -> bool:
        """Raise instead of accepting or refusing."""
        raise RuntimeError("synthetic accept_continuation failure")


def _build_handover_dispatcher(
    actor_class: type[_HandoverActor],
    snapshot: dict[DispatchId, Dispatch],
    *,
    continuation_accept_timeout: timedelta = timedelta(seconds=5),
) -> ActorDispatcher:
    """Create an `ActorDispatcher` wired for continuation handover tests."""

    async def factory(
        info: DispatchInfo, receiver: Receiver[DispatchInfo]
    ) -> _HandoverActor:
        return await actor_class.create(info, receiver)

    unused_channel = Broadcast[Dispatch](name="unused handover channel")
    return ActorDispatcher(
        actor_factory=factory,
        running_status_receiver=unused_channel.new_receiver(),
        continuation_predicate=is_exact_continuation,
        dispatches_snapshot=lambda: snapshot,
        continuation_accept_timeout=continuation_accept_timeout,
    )


@pytest.mark.parametrize("actor_class", [_HandoverActor, _RefusingHandoverActor])
async def test_rejected_handover_keeps_start_before_stop(
    generator: DispatchGenerator,
    actor_class: type[_HandoverActor],
    fake_time: time_machine.Traveller,
) -> None:
    """A non-capable or refused handover must not stop the predecessor first."""
    _HANDOVER_EVENTS.clear()
    start = _now()
    boundary = start + timedelta(hours=1)
    predecessor = _continuation_dispatch(generator, dispatch_id=1, start=start)
    successor = _continuation_dispatch(generator, dispatch_id=2, start=boundary)
    snapshot = {predecessor.id: predecessor, successor.id: successor}
    dispatcher = _build_handover_dispatcher(actor_class, snapshot)

    # pylint: disable=protected-access
    await dispatcher._handle_dispatch(predecessor)
    await asyncio.sleep(0)
    fake_time.move_to(boundary)
    # Same order as DispatchScheduler: the successor's start is always
    # processed before the predecessor's stop at the same instant.
    await dispatcher._handle_dispatch(successor)
    await asyncio.sleep(0)
    await dispatcher._handle_dispatch(_stopped(generator, predecessor))
    await asyncio.sleep(0)
    # pylint: enable=protected-access

    assert _HANDOVER_EVENTS[:3] == ["start actor-1", "start actor-2", "stop actor-1"]


@pytest.mark.parametrize(
    "actor_class", [_TimingOutHandoverActor, _RaisingHandoverActor]
)
async def test_unresponsive_handover_stops_predecessor_after_starting_successor(
    generator: DispatchGenerator,
    actor_class: type[_HandoverActor],
    fake_time: time_machine.Traveller,
) -> None:
    """A timed-out or raising handover starts the successor before stopping."""
    _HANDOVER_EVENTS.clear()
    start = _now()
    boundary = start + timedelta(hours=1)
    predecessor = _continuation_dispatch(generator, dispatch_id=1, start=start)
    successor = _continuation_dispatch(generator, dispatch_id=2, start=boundary)
    snapshot = {predecessor.id: predecessor, successor.id: successor}
    dispatcher = _build_handover_dispatcher(
        actor_class, snapshot, continuation_accept_timeout=timedelta(milliseconds=10)
    )

    # pylint: disable=protected-access
    await dispatcher._handle_dispatch(predecessor)
    await asyncio.sleep(0)
    fake_time.move_to(boundary)
    await dispatcher._handle_dispatch(successor)
    # pylint: enable=protected-access

    assert _HANDOVER_EVENTS[:3] == ["start actor-1", "start actor-2", "stop actor-1"]


async def test_accepted_handover_reuses_actor_without_warning(
    generator: DispatchGenerator,
    caplog: pytest.LogCaptureFixture,
    fake_time: time_machine.Traveller,
) -> None:
    """A successful handover reuses the actor and logs no WARNING."""
    _HANDOVER_EVENTS.clear()
    start = _now()
    boundary = start + timedelta(hours=1)
    predecessor = _continuation_dispatch(generator, dispatch_id=1, start=start)
    successor = _continuation_dispatch(generator, dispatch_id=2, start=boundary)
    snapshot = {predecessor.id: predecessor, successor.id: successor}
    dispatcher = _build_handover_dispatcher(_AcceptingHandoverActor, snapshot)

    with caplog.at_level(logging.DEBUG, logger="frequenz.dispatch._actor_dispatcher"):
        # pylint: disable=protected-access
        await dispatcher._handle_dispatch(predecessor)
        await asyncio.sleep(0)
        fake_time.move_to(boundary)
        await dispatcher._handle_dispatch(successor)
        await asyncio.sleep(0)
        await dispatcher._handle_dispatch(_stopped(generator, predecessor))
        await asyncio.sleep(0)
        # pylint: enable=protected-access

    assert _HANDOVER_EVENTS == [
        "start actor-1"
    ], "The actor must be reused, not restarted"
    assert not any(
        record.levelno >= logging.WARNING for record in caplog.records
    ), "A successful handover's predecessor stop must not log a WARNING"


async def test_continuation_chain_reuses_the_same_actor_twice(
    generator: DispatchGenerator,
    fake_time: time_machine.Traveller,
) -> None:
    """A chain of three back-to-back dispatches reuses the same actor twice."""
    _HANDOVER_EVENTS.clear()
    start = _now()
    boundary_1 = start + timedelta(hours=1)
    boundary_2 = boundary_1 + timedelta(hours=1)
    first = _continuation_dispatch(generator, dispatch_id=1, start=start)
    second = _continuation_dispatch(generator, dispatch_id=2, start=boundary_1)
    third = _continuation_dispatch(generator, dispatch_id=3, start=boundary_2)
    snapshot = {d.id: d for d in (first, second, third)}
    dispatcher = _build_handover_dispatcher(_AcceptingHandoverActor, snapshot)

    # pylint: disable=protected-access
    await dispatcher._handle_dispatch(first)
    await asyncio.sleep(0)
    fake_time.move_to(boundary_1)
    await dispatcher._handle_dispatch(second)
    await asyncio.sleep(0)
    fake_time.move_to(boundary_2)
    await dispatcher._handle_dispatch(third)
    await asyncio.sleep(0)
    actor_status = dispatcher._actors[DispatchActorId(third.id)]
    # pylint: enable=protected-access

    assert _HANDOVER_EVENTS == ["start actor-1"], "Only the first actor should start"
    assert actor_status.actor_and_channel is not None
    assert actor_status.actor_and_channel.current_dispatch.id == third.id


async def test_continuation_predicate_requires_matching_snapshot() -> None:
    """Refuse a continuation opt-in that can never take effect."""
    channel = Broadcast[Dispatch](name="unused handover channel")

    async def factory(
        info: DispatchInfo, receiver: Receiver[DispatchInfo]
    ) -> _HandoverActor:
        raise AssertionError("not called")

    with pytest.raises(ValueError):
        ActorDispatcher(
            actor_factory=factory,
            running_status_receiver=channel.new_receiver(),
            continuation_predicate=is_exact_continuation,
        )

    with pytest.raises(ValueError):
        ActorDispatcher(
            actor_factory=factory,
            running_status_receiver=channel.new_receiver(),
            dispatches_snapshot=lambda: {},
        )


async def test_stop_before_start_race_falls_back_to_normal_stop_start(
    generator: DispatchGenerator,
    fake_time: time_machine.Traveller,
) -> None:
    """A predecessor stop arriving before its successor's start is a plain stop.

    `DispatchScheduler` always sorts a start before a stop at the same
    instant, so this reverse order is a rare race (e.g. an update to the
    predecessor arriving right at its boundary). It must fall back to an
    ordinary stop/start, exactly like any other race: no continuation
    attempt, and no double payload delivery to either actor.
    """
    _HANDOVER_EVENTS.clear()
    start = _now()
    boundary = start + timedelta(hours=1)
    predecessor = _continuation_dispatch(generator, dispatch_id=1, start=start)
    successor = _continuation_dispatch(generator, dispatch_id=2, start=boundary)
    snapshot = {predecessor.id: predecessor, successor.id: successor}
    dispatcher = _build_handover_dispatcher(_AcceptingHandoverActor, snapshot)

    # pylint: disable=protected-access
    await dispatcher._handle_dispatch(predecessor)
    await asyncio.sleep(0)
    predecessor_status = dispatcher._actors[DispatchActorId(predecessor.id)]
    assert predecessor_status.actor_and_channel is not None
    predecessor_actor = cast(
        _AcceptingHandoverActor, predecessor_status.actor_and_channel.actor
    )
    fake_time.move_to(boundary)
    # The predecessor's stop arrives BEFORE the successor's start -- the
    # reverse of the scheduler's own ordering. The pair would otherwise be
    # an exact match: this proves no continuation is attempted either way.
    await dispatcher._handle_dispatch(_stopped(generator, predecessor))
    await asyncio.sleep(0)
    await dispatcher._handle_dispatch(successor)
    await asyncio.sleep(0)
    # pylint: enable=protected-access

    assert _HANDOVER_EVENTS == ["start actor-1", "stop actor-1", "start actor-2"]
    assert (
        predecessor_actor.accept_continuation_calls == 0
    ), "accept_continuation must never be called without a stop-first path"
