# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Dispatch type with support for next_run calculation."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

from frequenz.client.dispatch.types import Dispatch as BaseDispatch


@dataclass(frozen=True)
class Dispatch(BaseDispatch):
    """A dispatch type with extra functionality."""

    deleted: bool = False
    """Whether the dispatch is deleted."""

    def __init__(
        self,
        client_dispatch: BaseDispatch,
        deleted: bool = False,
    ):
        """Initialize the dispatch.

        Args:
            client_dispatch: The client dispatch.
            deleted: Whether the dispatch is deleted.
        """
        super().__init__(**client_dispatch.__dict__)
        # Work around frozen to set deleted
        object.__setattr__(self, "deleted", deleted)

    def _set_deleted(self) -> None:
        """Mark the dispatch as deleted."""
        object.__setattr__(self, "deleted", True)

    @property
    def started(self) -> bool:
        """Whether the dispatch is currently started."""
        now = datetime.now(tz=timezone.utc)
        return self.started_at(now)

    def started_at(self, now: datetime) -> bool:
        """Return whether the dispatch has started.

        A dispatch is considered started if the current time is after the start
        time but before the end time.

        Recurring dispatches are considered started if the current time is after
        the start time of the last occurrence but before the end time of the
        last occurrence.

        Args:
            now: The time to use as the current time.

        Returns:
            True if the dispatch has started at `now`, False otherwise.
        """
        if self.deleted:
            return False

        return super().started_at(now)

    def missed_runs(self, since: datetime) -> Iterator[datetime]:
        """Yield all missed runs of a dispatch.

        Args:
            since: The time to start checking for missed runs.

        Yields:
            The datetime of each missed run.
        """
        now = datetime.now(tz=timezone.utc)

        while (next_run := self.next_run_after(since)) and next_run < now:
            yield next_run
            since = next_run
