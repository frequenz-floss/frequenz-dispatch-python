# Dispatch Highlevel Interface Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

* The merge by type class now uses the correct logger path.
* The merge by type was made more robust under heavy load, making sure to use the same `now` for all dispatches that are checked.
* Fix that the merge filter was using an outdated dispatches dict once fetch() ran.
