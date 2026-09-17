# Dispatch Highlevel Interface Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Enhancements

- Improved docstring documentation across the project.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->

* Fixed documentation cross-references to dependencies by updating the
  mkdocstrings inventories to match the minimum dependency versions and
  adding the missing `frequenz-client-base` inventory.
* Declared the missing direct dependencies `frequenz-client-common`,
  `frequenz-core` and `grpcio`, which were previously only pulled in
  indirectly through `frequenz-client-dispatch`.
