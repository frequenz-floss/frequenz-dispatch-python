# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Helpers shared by dispatch merge handling."""

from collections.abc import Callable, Mapping
from datetime import datetime, timezone

from frequenz.client.dispatch.types import DispatchId

from ._dispatch import Dispatch


def _other_running_dispatches(
    dispatches: Mapping[DispatchId, Dispatch],
    dispatch_id: DispatchId,
    identity: object,
    dispatch_identity: Callable[[Dispatch], object],
    *,
    now: datetime | None = None,
) -> list[Dispatch]:
    """Return other running dispatches mapped to an identity."""
    if now is None:
        now = datetime.now(tz=timezone.utc)
    return [
        dispatch
        for dispatch in dispatches.values()
        if (
            dispatch.id != dispatch_id
            and dispatch.started_at(now)
            and dispatch_identity(dispatch) == identity
        )
    ]
