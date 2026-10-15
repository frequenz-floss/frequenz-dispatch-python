# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Opt-in support for exactly-adjacent, compatible dispatch continuations.

This module is deliberately separate from `_merge_strategies.py`: `MergeByType`
and `MergeByTypeTarget` merge dispatches that merely share (type[, target],
dry_run), with no regard for whether their time windows are adjacent or
overlapping. That is the wrong tool for "the next back-to-back dispatch window
is really a continuation of the previous one" -- a genuine, independent overlap
on the same target must NOT be treated as a continuation (see
`is_exact_continuation`). Nothing here changes `MergeByType`/`MergeByTypeTarget`
or `ActorDispatcher`'s default (per-dispatch-ID) behaviour; everything is opt-in.

There is deliberately no scheduler-side filter/strategy in this module. Every
dispatch stop and start is always forwarded as-is; `ActorDispatcher` is the
only place that decides what to do with it, using `continuation_predicate`.
See the module docstring of `_actor_dispatcher.py` for an overview and a
worked example.
"""

from __future__ import annotations

from frequenz.client.dispatch.types import (
    BatteryType,
    EvChargerType,
    InverterType,
    TargetCategories,
    TargetIds,
)

from ._dispatch import Dispatch


def _canonical_target(target: object) -> frozenset[tuple[object, ...]]:
    """Canonicalize a dispatch target into an order-independent, class-aware set.

    `TargetIds`/`TargetCategories` are already `frozenset`s, so their OWN
    `==` is already order-independent -- but comparing them via
    `tuple(target_a) == tuple(target_b)` throws that away and relies on two
    separately-constructed frozensets happening to iterate in the same order,
    which CPython does not guarantee for arbitrary construction paths. This
    canonicalizes to a plain `frozenset` of hashable keys instead, so
    comparison is unconditionally order-independent.

    For `TargetCategories`, each key is built from `TargetCategory.category2`
    (the library's OWN normalization of a category/type element to a single
    `ElectricalComponentCategory` value -- it deliberately treats the legacy
    `ComponentCategory.BATTERY` and `ElectricalComponentCategory.BATTERY` as
    the SAME real-world category, by value, and this canonicalization
    respects that rather than fighting it), plus, when the element names a
    specific sub-type (`BatteryType`/`EvChargerType`/`InverterType`), that
    sub-type's own class AND value. A plain "any battery" target is therefore
    NOT conflated with a specific "LI_ION battery" target (continuation
    requires the same specificity, not just the same broad category) --  but
    `ComponentCategory.BATTERY` and `ElectricalComponentCategory.BATTERY` on
    their own ARE treated as the same target, matching the library's own
    `category2` semantics. The sub-type is tagged with its own class name as
    well as its value so that, were any of `BatteryType`/`EvChargerType`/
    `InverterType` to ever become an `IntEnum` (today none are plain `Enum`,
    so this cannot yet happen), two different sub-types that happened to
    share a raw integer value still could not be conflated.

    Args:
        target: The dispatch target to canonicalize (a `TargetIds` or
            `TargetCategories` instance).

    Raises:
        TypeError: If `target` is neither a `TargetIds` nor a
            `TargetCategories`.

    Returns:
        An order-independent, class-aware, hashable representation of
            `target`, safe to compare with `==` against another call's result.
    """
    match target:
        case TargetIds():
            return frozenset(("id", int(component_id)) for component_id in target)
        case TargetCategories():
            keys: set[tuple[object, ...]] = set()
            for category in target:
                specific = (
                    category.target
                    if isinstance(
                        category.target, (BatteryType, EvChargerType, InverterType)
                    )
                    else None
                )
                keys.add(
                    (
                        "category",
                        category.category2,
                        type(specific).__qualname__ if specific is not None else None,
                        specific.value if specific is not None else None,
                    )
                )
            return frozenset(keys)
        case _:
            raise TypeError(
                f"Unsupported target type for continuation comparison: {type(target)!r}"
            )


def is_exact_continuation(predecessor: Dispatch, successor: Dispatch) -> bool:
    """Return whether `successor` exactly, compatibly continues `predecessor`.

    A successor continues a predecessor when, and only when:

      * they have the same dispatch `type`;
      * they have the same `dry_run` mode;
      * they resolve to the same `target` (compared order-independently);
      * the successor's CURRENT occurrence starts exactly when the
        predecessor's current occurrence ends -- a strict equality, not
        `<=`. For a recurring successor, this is its current occurrence's
        start, not its first one, so a daily pair of dispatches continues
        on every day, not only the first. A successor that starts before
        the predecessor ends is an OVERLAP, not a continuation, and must be
        treated as a fully independent dispatch.

    Args:
        predecessor: The dispatch that may be continued.
        successor: The dispatch that may continue it.

    Returns:
        True if `successor` is an exact, compatible continuation of `predecessor`.
    """
    if predecessor.type != successor.type:
        return False
    if predecessor.dry_run != successor.dry_run:
        return False
    if _canonical_target(predecessor.target) != _canonical_target(successor.target):
        return False

    if not predecessor.active:
        # An explicit cancellation is authoritative and is never silently
        # "continued through" by a later dispatch, even a coincidentally
        # adjacent one -- see the module docstring and `until`'s own behaviour
        # (it returns None once `active` is False).
        return False

    if predecessor.duration is None:
        # Runs indefinitely until explicitly stopped: there is no scheduled end
        # for a successor to be adjacent to. NOTE: `Dispatch.until` raises
        # ValueError for an active, duration-less dispatch rather than
        # returning None -- this check exists specifically to avoid calling it
        # in that case.
        return False

    predecessor_until = predecessor.until
    if predecessor_until is None:
        # Should not happen given the two guards above, but avoid ever treating
        # a missing boundary as a match.
        return False

    # For a recurring successor, `start_time` is only the start of its FIRST
    # occurrence. Use the start of its CURRENT occurrence instead, derived
    # the same way `predecessor_until` above is: from `until` (the end of
    # the occurrence running now) minus `duration`. A one-shot successor (or
    # one not currently active) falls back to `start_time`, which is
    # already its only/current occurrence.
    successor_start = successor.start_time
    if successor.duration is not None and (successor_until := successor.until):
        successor_start = successor_until - successor.duration
    return successor_start == predecessor_until
