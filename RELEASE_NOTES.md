# Dispatch Highlevel Interface Release Notes

## Upgrading

- The minimum `frequenz-channels` version will be 1.13.0. Until it is released, the dependency temporarily points at [PR 595](https://github.com/frequenz-floss/frequenz-channels-python/pull/595).
- The minimum `frequenz-core` version is now 1.6.0, and the minimum `typing-extensions` version is now 4.16.0. These versions are required by `frequenz-channels` 1.13.0 too.
- The internal timers now use `MonotonicTimer`, and channel shutdown uses `aclose`. These replace APIs that `frequenz-channels` v2 removes. The public dispatch API and behavior are unchanged.

## Enhancements

- Improved docstring documentation across the project.

## Bug Fixes

* Fixed documentation cross-references to dependencies by updating the mkdocstrings inventories to match the minimum dependency versions and adding the missing `frequenz-client-base` inventory.
* Declared the missing direct dependencies `frequenz-client-common`, `frequenz-core` and `grpcio`, which were previously only pulled in indirectly through `frequenz-client-dispatch`.
* Docstrings now use absolute cross-references to public paths instead of relative ones. Relative cross-references are expanded from the module where an object is defined since mkdocstrings-python 2.0.9, which broke some links, and projects rendering docstrings inherited from this library needed `relative_crossrefs: true` for them to work at all. See [frequenz-repo-config-python#655](https://github.com/frequenz-floss/frequenz-repo-config-python/issues/655) for details.
