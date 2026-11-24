# Dispatch Highlevel Interface Release Notes

## Summary

This release adds opt-in support for handing over a running actor between exactly adjacent dispatches, and adds support for `frequenz-channels` v2.0.0, while still supporting v1.13. It also fixes actors being left running when a dispatch's merge identity changes, and a scheduler timer crash.

## Upgrading

* `ActorDispatcher.ActorAndChannel` was exposed by mistake. This internal helper is now private and is no longer available under its old name.
* Dispatch now accepts `frequenz-channels >= 1.13.0, < 3`, including v2.
* The minimum `frequenz-core` version is now 1.6.0, and the minimum `typing-extensions` version is now 4.16.0. These versions are required by `frequenz-channels` 1.13.0 too.
* The internal timers now use `MonotonicTimer`, and channel shutdown uses `aclose`. These replace APIs that `frequenz-channels` v2 removes.

## New Features

* Added opt-in support for reusing a continuation-capable actor across exactly adjacent, compatible dispatches, enabled only through `Dispatcher.start_managing(continuation_predicate=...)`. New public names: `ContinuationCapable`, `is_exact_continuation()`, `DispatchInfo.continuation_of` and `DispatchInfo.dispatch`. Ambiguous or unsupported handovers keep the normal stop/start behavior.

## Bug Fixes

* Docstrings now use absolute cross-references to public paths instead of relative ones. Relative cross-references are expanded from the module where an object is defined since mkdocstrings-python 2.0.9, which broke some links, and projects rendering docstrings inherited from this library needed `relative_crossrefs: true` for them to work at all. See [frequenz-repo-config-python#655](https://github.com/frequenz-floss/frequenz-repo-config-python/issues/655) for details.
* Fixed actors being left running when an update changes a dispatch's merge identity.
* Fixed the scheduler's timer crashing when two scheduled events landed at the same instant, and stopped it from spinning at a microsecond interval once its event queue emptied afterward.
