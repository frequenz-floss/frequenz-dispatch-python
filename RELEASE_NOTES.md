# Dispatch Highlevel Interface Release Notes

## Enhancements

- Improved docstring documentation across the project.

## Bug Fixes

* Fixed documentation cross-references to dependencies by updating the mkdocstrings inventories to match the minimum dependency versions and adding the missing `frequenz-client-base` inventory.
* Declared the missing direct dependencies `frequenz-client-common`, `frequenz-core` and `grpcio`, which were previously only pulled in indirectly through `frequenz-client-dispatch`.
