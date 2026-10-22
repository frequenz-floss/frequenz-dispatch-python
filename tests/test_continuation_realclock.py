# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Real-clock, real-scheduler integration tests for the continuation prototype.

Deliberately NO `time_machine`, NO `async_solipsism`: this uses the DEFAULT
asyncio event loop and genuine `datetime.now()`, with SHORT real dispatch
durations (a few hundred milliseconds), so the whole suite still runs in a
few seconds while every clock involved -- `asyncio`'s own loop time and
`DispatchScheduler`'s `Timer` -- is the SAME, single, real clock. This sidesteps
the `time_machine`/`async_solipsism`/`frequenz.channels.timer.Timer`
interaction that made `test_continuation_integration.py` unreliable; it is not
a replacement for that file's intent (real `Dispatcher.start_managing` + real
`DispatchScheduler` + the official `FakeClient` test double, end-to-end,
exercising deletion, ambiguity and coincident boundaries), just a different,
real-time way of driving the same real components.

Assertions are event-based (`asyncio.wait_for` polling a condition), never a
fixed sleep-and-hope.

Synthetic component ID 1 only. No network/API/host calls.
"""

# pylint: disable=protected-access

import asyncio
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import patch

import pytest
from frequenz.channels import Receiver
from frequenz.client.common.microgrid import MicrogridId
from frequenz.client.dispatch.test.client import FakeClient
from frequenz.client.dispatch.types import TargetIds
from frequenz.sdk.actor import Actor

from frequenz.dispatch import Dispatcher, DispatchInfo, is_exact_continuation
from frequenz.dispatch._actor_dispatcher import ActorDispatcher, DispatchActorId

REAL_EVENT_LOOP_POLICY = asyncio.get_event_loop_policy()


@pytest.fixture
def event_loop_policy() -> Generator[asyncio.AbstractEventLoopPolicy, None, None]:
    """Pin the real default asyncio event loop policy for this file.

    `test_frequenz_dispatch.py` defines an `event_loop_policy` fixture that
    calls `asyncio.set_event_loop_policy(async_solipsism.EventLoopPolicy())`
    -- a GLOBAL, process-wide mutation it never restores. pytest-asyncio's own
    default `event_loop_policy` fixture just returns whatever the current
    global policy is, so once that other file's tests have run in the same
    process, every later file silently inherits a solipsism (manually-ticked,
    never-really-advancing) loop -- exactly the shape of hang this file is
    designed not to have. Defining this fixture here pins the real default
    regardless of what ran before, independent of execution order.  Restore
    the previous global policy explicitly so this fixture cannot leak its own
    process-wide mutation into tests that run after this file.
    """
    previous_policy = asyncio.get_event_loop_policy()
    asyncio.set_event_loop_policy(REAL_EVENT_LOOP_POLICY)
    try:
        yield REAL_EVENT_LOOP_POLICY
    finally:
        asyncio.set_event_loop_policy(previous_policy)


# The real dispatch API (via FakeClient, matching the real protobuf wire
# format) rounds `start_time` to the nearest second and `duration` with
# `round(seconds)` (confirmed empirically: a 400ms duration round-trips as
# exactly 0, a sub-second start_time has its microseconds truncated) -- so
# anything sub-second here is silently lossy. These are whole seconds,
# deliberately: short enough for a fast suite, long enough to round-trip
# exactly.
DURATION = timedelta(seconds=2)
STARTUP_MARGIN = timedelta(seconds=1)


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


async def _wait_until(
    predicate: Any, *, timeout: float = 3.0, interval: float = 0.01
) -> None:
    """Poll `predicate()` (real, short sleeps) until true, or raise."""

    async def _poll() -> None:
        while not predicate():
            await asyncio.sleep(interval)

    await asyncio.wait_for(_poll(), timeout=timeout)


class RecordingActor(Actor):
    """Track the dispatch ID, received updates, and proposed power.

    The power is a `target_power_w` payload field stored directly. This actor
    is NOT continuation-aware: it has no
    `accept_continuation` method, so `isinstance(actor, ContinuationCapable)`
    is `False`.
    """

    def __init__(
        self, initial_dispatch: DispatchInfo, receiver: Receiver[DispatchInfo]
    ) -> None:
        """Initialize the recording actor."""
        super().__init__(name=f"actor-for-{initial_dispatch._src.id}")
        self.current_dispatch_id = initial_dispatch._src.id
        self.proposed_power_w: float | None = initial_dispatch.options.get(
            "target_power_w"
        )
        self.stop_called = False
        self.receiver = receiver

    async def _run(self) -> None:
        async for update in self.receiver:
            if update._src.id == self.current_dispatch_id:
                self.proposed_power_w = update.options.get("target_power_w")

    async def stop(self, msg: str | None = None) -> None:
        """Stop, recording that it happened and withdrawing (like a real actor)."""
        self.stop_called = True
        self.proposed_power_w = None
        await super().stop(msg)

    @classmethod
    async def create(
        cls, initial_dispatch: DispatchInfo, receiver: Receiver[DispatchInfo]
    ) -> "RecordingActor":
        """Create a new actor."""
        return cls(initial_dispatch, receiver)


class ContinuationAwareActor(RecordingActor):
    """Implements `ContinuationCapable.accept_continuation`."""

    async def accept_continuation(self, update: DispatchInfo) -> bool:
        """Accept a continuation update, immediately applying its payload."""
        if update.continuation_of != self.current_dispatch_id:
            return False
        self.current_dispatch_id = update._src.id
        self.proposed_power_w = update.options.get("target_power_w")
        return True


@dataclass
class _Env:
    dispatcher: Dispatcher
    client: FakeClient
    microgrid_id: MicrogridId


@pytest.fixture
async def env() -> _Env:
    """Create a dispatcher on the real-time asyncio event loop.

    The dispatcher is backed by the official `FakeClient`, with no
    time-machine or async-solipsism clock.
    """

    class _FakeClientWithSignature(FakeClient):
        # pylint: disable=too-many-arguments,unused-argument
        def __init__(
            self,
            *,
            server_url: str,
            auth_key: str | None = None,
            key: str | None = None,
            sign_secret: str | None = None,
            call_timeout: timedelta,
            stream_timeout: timedelta,
        ) -> None:
            super().__init__()

        # pylint: enable=too-many-arguments,unused-argument

    mid = MicrogridId(1)
    with patch(
        "frequenz.dispatch._dispatcher.DispatchApiClient", _FakeClientWithSignature
    ):
        dispatcher = Dispatcher(
            microgrid_id=mid, server_url="grpc://test-url", key="test-key"
        )
        dispatcher.start()
        await asyncio.sleep(0.2)  # let its background _run() task actually start
    return _Env(
        dispatcher=dispatcher,
        client=cast(FakeClient, dispatcher.client),
        microgrid_id=mid,
    )


def _actor_dispatcher_for(
    dispatcher: Dispatcher, dispatch_type: str
) -> ActorDispatcher:
    return dispatcher._actor_dispatchers[dispatch_type]


async def test_exact_adjacency_end_to_end_real_scheduler(env: _Env) -> None:
    """Continue an exactly adjacent dispatch with the real scheduler.

    The predecessor naturally starts, naturally reaches its
    own scheduled end, and the real scheduler's own Timer fires the stop --
    which `ActorDispatcher` resolves into a continuation using the real
    `dispatches_snapshot`, all without this test ever manually constructing
    or feeding any event.
    """
    dispatch_type = "REALCLOCK_EXACT_ADJACENCY"
    await env.dispatcher.start_managing(
        dispatch_type=dispatch_type,
        actor_factory=ContinuationAwareActor.create,
        continuation_predicate=is_exact_continuation,
    )
    actor_dispatcher = _actor_dispatcher_for(env.dispatcher, dispatch_type)

    start = _now() + STARTUP_MARGIN
    boundary = start + DURATION
    predecessor = await env.client.create(
        microgrid_id=env.microgrid_id,
        type=dispatch_type,
        start_time=start,
        duration=DURATION,
        target=TargetIds(1),
        active=True,
        dry_run=False,
        payload={"target_power_w": 1_200_000.0},
    )
    successor = await env.client.create(
        microgrid_id=env.microgrid_id,
        type=dispatch_type,
        start_time=boundary,
        duration=None,
        target=TargetIds(1),
        active=True,
        dry_run=False,
        payload={"target_power_w": 900_000.0},  # deliberately different
    )

    await _wait_until(
        lambda: DispatchActorId(predecessor.id) in actor_dispatcher._actors
    )
    actor = cast(
        ContinuationAwareActor,
        actor_dispatcher._actors[DispatchActorId(predecessor.id)].actor,
    )
    await _wait_until(lambda: actor.proposed_power_w == 1_200_000.0)

    # Real time passes naturally past `boundary` -- the real scheduler's own
    # Timer (with the floor-fix) fires the real stop event for `predecessor`.
    await _wait_until(
        lambda: actor.current_dispatch_id == successor.id
        and actor.proposed_power_w == 900_000.0,
        timeout=5.0,
    )

    assert (
        not actor.stop_called
    ), "Expected the actor to be reused via continuation, never stopped"
    assert len(actor_dispatcher._actors) == 1, "Expected exactly one live actor, reused"


async def test_successor_deleted_after_continuation_real_scheduler(env: _Env) -> None:
    """Stop the reused actor when its successor is deleted."""
    dispatch_type = "REALCLOCK_SUCCESSOR_DELETED"
    await env.dispatcher.start_managing(
        dispatch_type=dispatch_type,
        actor_factory=ContinuationAwareActor.create,
        continuation_predicate=is_exact_continuation,
    )
    actor_dispatcher = _actor_dispatcher_for(env.dispatcher, dispatch_type)

    start = _now() + STARTUP_MARGIN
    boundary = start + DURATION
    predecessor = await env.client.create(
        microgrid_id=env.microgrid_id,
        type=dispatch_type,
        start_time=start,
        duration=DURATION,
        target=TargetIds(1),
        active=True,
        dry_run=False,
        payload={},
    )
    successor = await env.client.create(
        microgrid_id=env.microgrid_id,
        type=dispatch_type,
        start_time=boundary,
        duration=None,
        target=TargetIds(1),
        active=True,
        dry_run=False,
        payload={},
    )

    await _wait_until(
        lambda: DispatchActorId(predecessor.id) in actor_dispatcher._actors
    )
    await _wait_until(
        lambda: len(actor_dispatcher._actors) == 1
        and next(iter(actor_dispatcher._actors.values())).current_dispatch.id
        == successor.id,
        timeout=5.0,
    )
    reused_actor = cast(
        ContinuationAwareActor, next(iter(actor_dispatcher._actors.values())).actor
    )

    await env.client.delete(microgrid_id=env.microgrid_id, dispatch_id=successor.id)

    await _wait_until(lambda: reused_actor.stop_called, timeout=3.0)
    await _wait_until(lambda: len(actor_dispatcher._actors) == 0, timeout=3.0)


async def test_ambiguous_successor_real_scheduler(env: _Env) -> None:
    """Refuse two successors that both continue the same predecessor.

    The scheduler orders START before STOP at a shared instant.  Pairwise
    uniqueness therefore has to reject this ambiguity on the start side,
    before either successor can arbitrarily claim the predecessor's actor.
    Both successors must start independently and the predecessor must stop,
    withdrawing its old proposal.
    """
    dispatch_type = "REALCLOCK_AMBIGUOUS_SUCCESSOR"
    await env.dispatcher.start_managing(
        dispatch_type=dispatch_type,
        actor_factory=ContinuationAwareActor.create,
        continuation_predicate=is_exact_continuation,
    )
    actor_dispatcher = _actor_dispatcher_for(env.dispatcher, dispatch_type)

    start = _now() + STARTUP_MARGIN
    boundary = start + DURATION
    predecessor = await env.client.create(
        microgrid_id=env.microgrid_id,
        type=dispatch_type,
        start_time=start,
        duration=DURATION,
        target=TargetIds(1),
        active=True,
        dry_run=False,
        payload={"target_power_w": 1_200_000.0},
    )
    successor_a = await env.client.create(
        microgrid_id=env.microgrid_id,
        type=dispatch_type,
        start_time=boundary,
        duration=None,
        target=TargetIds(1),
        active=True,
        dry_run=False,
        payload={"target_power_w": 900_000.0},
    )
    successor_b = await env.client.create(
        microgrid_id=env.microgrid_id,
        type=dispatch_type,
        start_time=boundary,
        duration=None,
        target=TargetIds(1),
        active=True,
        dry_run=False,
        payload={"target_power_w": 800_000.0},
    )

    await _wait_until(
        lambda: DispatchActorId(predecessor.id) in actor_dispatcher._actors
    )
    predecessor_actor = cast(
        ContinuationAwareActor,
        actor_dispatcher._actors[DispatchActorId(predecessor.id)].actor,
    )

    # Whichever race resolution occurs (see the docstring above), it must
    # settle into EXACTLY two live actors/identities, one correctly
    # representing each successor's own payload -- never fewer (an orphan)
    # and never more (a double-claim), and never both successors pointing at
    # the SAME actor (a corrupted shared re-key).
    await _wait_until(
        lambda: len(actor_dispatcher._actors) == 2
        and DispatchActorId(successor_a.id) in actor_dispatcher._actors
        and DispatchActorId(successor_b.id) in actor_dispatcher._actors,
        timeout=5.0,
    )

    actor_a = cast(
        ContinuationAwareActor,
        actor_dispatcher._actors[DispatchActorId(successor_a.id)].actor,
    )
    actor_b = cast(
        ContinuationAwareActor,
        actor_dispatcher._actors[DispatchActorId(successor_b.id)].actor,
    )
    assert (
        actor_a is not actor_b
    ), "The two successors must never share the same re-keyed actor"
    assert predecessor_actor is not actor_a
    assert predecessor_actor is not actor_b
    assert predecessor_actor.stop_called
    assert predecessor_actor.proposed_power_w is None
    assert actor_a.current_dispatch_id == successor_a.id
    assert actor_b.current_dispatch_id == successor_b.id
    assert actor_a.proposed_power_w == 900_000.0
    assert actor_b.proposed_power_w == 800_000.0


async def test_ambiguous_predecessor_real_scheduler(env: _Env) -> None:
    """Refuse two predecessors that both match one successor.

    The successor must start independently, then both predecessor actors must
    stop and withdraw their own proposals.  Stop-side processing must not
    allow the second predecessor to claim the successor merely because the
    first predecessor has already been stopped.
    """
    dispatch_type = "REALCLOCK_AMBIGUOUS_PREDECESSOR"
    await env.dispatcher.start_managing(
        dispatch_type=dispatch_type,
        actor_factory=ContinuationAwareActor.create,
        continuation_predicate=is_exact_continuation,
    )
    actor_dispatcher = _actor_dispatcher_for(env.dispatcher, dispatch_type)

    start = _now() + STARTUP_MARGIN
    boundary = start + DURATION
    predecessor_a = await env.client.create(
        microgrid_id=env.microgrid_id,
        type=dispatch_type,
        start_time=start,
        duration=DURATION,
        target=TargetIds(1),
        active=True,
        dry_run=False,
        payload={"target_power_w": 1_200_000.0},
    )
    predecessor_b = await env.client.create(
        microgrid_id=env.microgrid_id,
        type=dispatch_type,
        start_time=start,
        duration=DURATION,
        target=TargetIds(1),
        active=True,
        dry_run=False,
        payload={"target_power_w": 800_000.0},
    )
    successor = await env.client.create(
        microgrid_id=env.microgrid_id,
        type=dispatch_type,
        start_time=boundary,
        duration=None,
        target=TargetIds(1),
        active=True,
        dry_run=False,
        payload={"target_power_w": 900_000.0},
    )

    await _wait_until(
        lambda: DispatchActorId(predecessor_a.id) in actor_dispatcher._actors
        and DispatchActorId(predecessor_b.id) in actor_dispatcher._actors
    )
    actor_a = cast(
        ContinuationAwareActor,
        actor_dispatcher._actors[DispatchActorId(predecessor_a.id)].actor,
    )
    actor_b = cast(
        ContinuationAwareActor,
        actor_dispatcher._actors[DispatchActorId(predecessor_b.id)].actor,
    )

    await _wait_until(
        lambda: len(actor_dispatcher._actors) == 1
        and DispatchActorId(successor.id) in actor_dispatcher._actors,
        timeout=5.0,
    )
    successor_actor = cast(
        ContinuationAwareActor,
        actor_dispatcher._actors[DispatchActorId(successor.id)].actor,
    )

    assert successor_actor is not actor_a
    assert successor_actor is not actor_b
    assert successor_actor.current_dispatch_id == successor.id
    assert successor_actor.proposed_power_w == 900_000.0
    assert actor_a.stop_called and actor_a.proposed_power_w is None
    assert actor_b.stop_called and actor_b.proposed_power_w is None


async def test_snapshot_predecessor_without_actor_still_makes_pair_ambiguous(
    env: _Env,
) -> None:
    """Count snapshot predecessors even when one actor failed to start.

    Start-side matching must not reuse the sole registered predecessor actor
    when another known dispatch also matches the successor but has no actor.
    """
    dispatch_type = "REALCLOCK_SNAPSHOT_ONLY_PREDECESSOR"

    async def selective_factory(
        initial_dispatch: DispatchInfo, receiver: Receiver[DispatchInfo]
    ) -> ContinuationAwareActor:
        if initial_dispatch.options.get("fail_start"):
            raise RuntimeError("synthetic actor start failure")
        return cast(
            ContinuationAwareActor,
            await ContinuationAwareActor.create(initial_dispatch, receiver),
        )

    await env.dispatcher.start_managing(
        dispatch_type=dispatch_type,
        actor_factory=selective_factory,
        continuation_predicate=is_exact_continuation,
    )
    actor_dispatcher = _actor_dispatcher_for(env.dispatcher, dispatch_type)

    start = _now() + STARTUP_MARGIN
    boundary = start + DURATION
    registered_predecessor = await env.client.create(
        microgrid_id=env.microgrid_id,
        type=dispatch_type,
        start_time=start,
        duration=DURATION,
        target=TargetIds(1),
        active=True,
        dry_run=False,
        payload={"target_power_w": 1_200_000.0},
    )
    snapshot_only_predecessor = await env.client.create(
        microgrid_id=env.microgrid_id,
        type=dispatch_type,
        start_time=start,
        duration=DURATION,
        target=TargetIds(1),
        active=True,
        dry_run=False,
        payload={"target_power_w": 800_000.0, "fail_start": True},
    )
    successor = await env.client.create(
        microgrid_id=env.microgrid_id,
        type=dispatch_type,
        start_time=boundary,
        duration=None,
        target=TargetIds(1),
        active=True,
        dry_run=False,
        payload={"target_power_w": 900_000.0},
    )

    registered_identity = DispatchActorId(registered_predecessor.id)
    failed_identity = DispatchActorId(snapshot_only_predecessor.id)
    await _wait_until(
        lambda: registered_identity in actor_dispatcher._actors
        and failed_identity in actor_dispatcher._failed_dispatches
    )
    predecessor_actor = cast(
        ContinuationAwareActor,
        actor_dispatcher._actors[registered_identity].actor,
    )

    await _wait_until(
        lambda: len(actor_dispatcher._actors) == 1
        and DispatchActorId(successor.id) in actor_dispatcher._actors,
        timeout=5.0,
    )
    successor_actor = cast(
        ContinuationAwareActor,
        actor_dispatcher._actors[DispatchActorId(successor.id)].actor,
    )

    assert successor_actor is not predecessor_actor
    assert successor_actor.current_dispatch_id == successor.id
    assert successor_actor.proposed_power_w == 900_000.0
    assert predecessor_actor.stop_called
    assert predecessor_actor.proposed_power_w is None
