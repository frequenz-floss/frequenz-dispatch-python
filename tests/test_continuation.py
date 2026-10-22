# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Unit tests for `is_exact_continuation`'s target-equality semantics.

These are pure, synchronous tests of `_canonical_target`/`is_exact_continuation`
-- no scheduler, no actors, no event loop. Synthetic IDs only.
"""

from dataclasses import replace
from datetime import datetime, timedelta, timezone

from frequenz.client.common.microgrid.components import ComponentCategory
from frequenz.client.common.microgrid.electrical_components import (
    ElectricalComponentCategory,
)
from frequenz.client.dispatch.recurrence import RecurrenceRule
from frequenz.client.dispatch.test.generator import DispatchGenerator
from frequenz.client.dispatch.types import (
    BatteryType,
    DispatchId,
    EvChargerType,
    TargetCategories,
    TargetIds,
)

from frequenz.dispatch import Dispatch, is_exact_continuation
from frequenz.dispatch._continuation import _canonical_target


def _make_dispatch(
    dispatch_id: int,
    *,
    start_time: datetime,
    duration: timedelta | None,
    target: TargetIds | TargetCategories,
    active: bool = True,
    dry_run: bool = False,
    dispatch_type: str = "TEST",
) -> Dispatch:
    # pylint: disable=too-many-arguments
    generator = DispatchGenerator()
    spec = replace(
        generator.generate_dispatch(),
        id=DispatchId(dispatch_id),
        type=dispatch_type,
        start_time=start_time,
        duration=duration,
        target=target,
        active=active,
        dry_run=dry_run,
        recurrence=RecurrenceRule(),
    )
    return Dispatch(spec)


UTC = timezone.utc


# --- _canonical_target: order independence ---


def test_reordered_target_ids_are_equal() -> None:
    """Canonicalize reordered `TargetIds` equally."""
    assert _canonical_target(TargetIds(1, 2, 3)) == _canonical_target(
        TargetIds(3, 1, 2)
    )
    assert _canonical_target(TargetIds(5, 7)) == _canonical_target(TargetIds(7, 5))


def test_reordered_target_categories_are_equal() -> None:
    """Same for a multi-element `TargetCategories`."""
    a = TargetCategories(
        ElectricalComponentCategory.BATTERY, ElectricalComponentCategory.INVERTER
    )
    b = TargetCategories(
        ElectricalComponentCategory.INVERTER, ElectricalComponentCategory.BATTERY
    )
    assert _canonical_target(a) == _canonical_target(b)


# --- _canonical_target: class-aware, not bare-value, equality ---


def test_legacy_and_new_category_enum_for_the_same_category_are_equal() -> None:
    """Canonicalize legacy and current spellings of a category equally."""
    legacy = TargetCategories(ComponentCategory.BATTERY)
    current = TargetCategories(ElectricalComponentCategory.BATTERY)
    assert _canonical_target(legacy) == _canonical_target(current)


def test_same_numeric_value_different_kind_is_not_equal() -> None:
    """Keep equal numeric values of different target kinds distinct.

    A plain category target and a MORE SPECIFIC sub-type target must NOT be
    conflated, even when the broad category matches. A `TargetIds`
    whose id happens to numerically match a category's enum value must not be
    conflated with that category either (different container kind entirely).
    """
    plain_battery = TargetCategories(ElectricalComponentCategory.BATTERY)
    specific_li_ion = TargetCategories(BatteryType.LI_ION)
    assert _canonical_target(plain_battery) != _canonical_target(
        specific_li_ion
    ), "A plain 'any battery' target must not equal a specific battery sub-type target"

    battery_value = ElectricalComponentCategory.BATTERY.value
    ids_with_matching_number = TargetIds(battery_value)
    assert _canonical_target(ids_with_matching_number) != _canonical_target(
        plain_battery
    ), (
        "A TargetIds whose id numerically matches a category's enum value must "
        "not be conflated with that category -- different container kind"
    )


def test_different_specific_subtypes_are_not_equal() -> None:
    """Two different, equally-specific sub-types are not conflated with each other."""
    li_ion = TargetCategories(BatteryType.LI_ION)
    assert _canonical_target(li_ion) != _canonical_target(
        TargetCategories(EvChargerType.AC)
    )


# --- is_exact_continuation: end-to-end with the target comparison ---


def test_continuation_allowed_across_legacy_and_new_category_enum() -> None:
    """Allow continuation across legacy and current category spellings."""
    boundary = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    predecessor = _make_dispatch(
        1,
        start_time=boundary - timedelta(minutes=10),
        duration=timedelta(minutes=10),
        target=TargetCategories(ComponentCategory.BATTERY),
    )
    successor = _make_dispatch(
        2,
        start_time=boundary,
        duration=None,
        target=TargetCategories(ElectricalComponentCategory.BATTERY),
    )
    assert is_exact_continuation(predecessor, successor)


def test_continuation_refused_for_differently_specific_targets() -> None:
    """Refuse continuation between differently specific targets."""
    boundary = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    predecessor = _make_dispatch(
        1,
        start_time=boundary - timedelta(minutes=10),
        duration=timedelta(minutes=10),
        target=TargetCategories(ElectricalComponentCategory.BATTERY),
    )
    successor = _make_dispatch(
        2,
        start_time=boundary,
        duration=None,
        target=TargetCategories(BatteryType.LI_ION),
    )
    assert not is_exact_continuation(predecessor, successor)


def test_continuation_allowed_with_reordered_multi_id_target() -> None:
    """Allow continuation with reordered component IDs."""
    boundary = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    predecessor = _make_dispatch(
        1,
        start_time=boundary - timedelta(minutes=10),
        duration=timedelta(minutes=10),
        target=TargetIds(1, 2, 3),
    )
    successor = _make_dispatch(
        2,
        start_time=boundary,
        duration=None,
        target=TargetIds(3, 2, 1),
    )
    assert is_exact_continuation(predecessor, successor)
