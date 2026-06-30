# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Dispatch lifecycle events."""

from dataclasses import dataclass

from ._dispatch import Dispatch


@dataclass(frozen=True)
class Created:
    """A dispatch created event."""

    dispatch: Dispatch
    """The dispatch that was created."""


@dataclass(frozen=True)
class Updated:
    """A dispatch updated event."""

    dispatch: Dispatch
    """The dispatch that was updated."""


@dataclass(frozen=True)
class Deleted:
    """A dispatch deleted event."""

    dispatch: Dispatch
    """The dispatch that was deleted."""


DispatchEvent = Created | Updated | Deleted
"""The type representing dispatch lifecycle events.

This type is used to send dispatches that were created, updated or deleted
over the channel.
"""
