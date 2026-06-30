# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""A high-level interface for the dispatch API.

A small overview of the most important classes in this module:

* [`Dispatcher`][.Dispatcher]: The entry point for the API.
* [`Dispatch`][.Dispatch]: A dispatch type with lots of useful extra functionality.
* [`ActorDispatcher`][.ActorDispatcher]: A service to manage other actors based on
  incoming dispatches.
* [`Created`][.Created],
  [`Updated`][.Updated],
  [`Deleted`][.Deleted]: Dispatch event types.

"""

from ._actor_dispatcher import ActorDispatcher, DispatchInfo
from ._bg_service import MergeStrategy
from ._dispatch import Dispatch
from ._dispatcher import Dispatcher
from ._event import Created, Deleted, DispatchEvent, Updated
from ._merge_strategies import MergeByType, MergeByTypeTarget

__all__ = [
    "Created",
    "Deleted",
    "DispatchEvent",
    "Dispatcher",
    "Updated",
    "Dispatch",
    "ActorDispatcher",
    "DispatchInfo",
    "MergeStrategy",
    "MergeByType",
    "MergeByTypeTarget",
]
