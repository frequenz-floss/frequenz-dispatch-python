# Dispatch Highlevel Interface Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

* `ActorDispatcher.ActorAndChannel` is no longer frozen and has a new required `current_dispatch` field. Code that builds it directly will break; use `ActorDispatcher` itself instead.

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

* Added opt-in support for reusing a continuation-capable actor across exactly adjacent, compatible dispatches, enabled only through `Dispatcher.start_managing(continuation_predicate=...)`. New public names: `ContinuationCapable`, `is_exact_continuation()`, `DispatchInfo.continuation_of` and `DispatchInfo.dispatch`. Ambiguous or unsupported handovers keep the normal stop/start behavior.

## Bug Fixes

* Docstrings now use absolute cross-references to public paths instead of relative ones. Relative cross-references are expanded from the module where an object is defined since mkdocstrings-python 2.0.9, which broke some links, and projects rendering docstrings inherited from this library needed `relative_crossrefs: true` for them to work at all. See [frequenz-repo-config-python#655](https://github.com/frequenz-floss/frequenz-repo-config-python/issues/655) for details.
* Fixed actors being left running when an update changes a dispatch's merge identity.
* Fixed the scheduler's timer crashing when two scheduled events landed at the same instant, and stopped it from spinning at a microsecond interval once its event queue emptied afterward.
