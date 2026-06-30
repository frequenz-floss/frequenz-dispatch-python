# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Different merge strategies for dispatch running state events."""

import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from sys import maxsize
from typing import Any

from frequenz.client.dispatch.types import DispatchId
from typing_extensions import override

from ._actor_dispatcher import DispatchActorId
from ._bg_service import MergeStrategy
from ._dispatch import Dispatch

_logger = logging.getLogger(__name__)


def _hash_positive(args: Any) -> int:
    """Make a positive hash."""
    return hash(args) + maxsize + 1


class MergeByType(MergeStrategy):
    """A merge strategy that combines running intervals based on dispatch type."""

    @override
    def identity(self, dispatch: Dispatch) -> DispatchActorId:
        """Return the actor identity for a dispatch based on its type.

        Args:
            dispatch: The dispatch to compute the identity for.

        Returns:
            An identity value grouping dispatches with the same type and dry-run flag.
        """
        return DispatchActorId(_hash_positive((dispatch.type, dispatch.dry_run)))

    @override
    def filter(
        self, dispatches: Mapping[DispatchId, Dispatch], dispatch: Dispatch
    ) -> bool:
        """Return whether the dispatch event should be propagated.

        Start events are always propagated. Stop events are only propagated
        if no other dispatch matching this strategy's criteria is still running.

        Args:
            dispatches: The currently known dispatches, keyed by their ID.
            dispatch: The dispatch event to evaluate.

        Returns:
            `True` if the event should be forwarded to consumers, `False` otherwise.
        """
        now = datetime.now(tz=timezone.utc)

        if dispatch.started_at(now):
            _logger.debug("Keeping start event %s", dispatch.id)
            return True

        running_dispatch_list = [
            existing_dispatch
            for existing_dispatch in dispatches.values()
            if (
                self.identity(existing_dispatch) == self.identity(dispatch)
                and existing_dispatch.id != dispatch.id
            )
        ]

        other_dispatches_running = any(
            running_dispatch.started_at(now)
            for running_dispatch in running_dispatch_list
        )

        _logger.debug(
            "%s stop event %s because other_dispatches_running=%s",
            "Ignoring" if other_dispatches_running else "Allowing",
            dispatch.id,
            other_dispatches_running,
        )

        if other_dispatches_running:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    "Active other dispatches: %s",
                    list(
                        running_dispatch.id
                        for running_dispatch in running_dispatch_list
                    ),
                )

        return not other_dispatches_running


class MergeByTypeTarget(MergeByType):
    """A merge strategy that combines running intervals based on dispatch type and target."""

    @override
    def identity(self, dispatch: Dispatch) -> DispatchActorId:
        """Return the actor identity for a dispatch based on its type and target.

        Args:
            dispatch: The dispatch to compute the identity for.

        Returns:
            An identity value grouping dispatches with the same type, dry-run flag,
            and target.
        """
        return DispatchActorId(
            _hash_positive((dispatch.type, dispatch.dry_run, tuple(dispatch.target)))
        )
