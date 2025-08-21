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
    """Merge running intervals based on the dispatch type."""

    @override
    def identity(self, dispatch: Dispatch) -> DispatchActorId:
        """Identity function for the merge criteria."""
        return DispatchActorId(_hash_positive((dispatch.type, dispatch.dry_run)))

    @override
    def filter(
        self, dispatches: Mapping[DispatchId, Dispatch], dispatch: Dispatch
    ) -> bool:
        """Filter dispatches based on the merge strategy.

        Keeps start events.
        Keeps stop events only if no other dispatches matching the
        strategy's criteria are running.
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
    """Merge running intervals based on the dispatch type and target."""

    @override
    def identity(self, dispatch: Dispatch) -> DispatchActorId:
        """Identity function for the merge criteria."""
        return DispatchActorId(
            _hash_positive((dispatch.type, dispatch.dry_run, tuple(dispatch.target)))
        )
