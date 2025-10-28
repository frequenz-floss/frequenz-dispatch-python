# Dispatch Highlevel Interface Release Notes

## Summary

This is the first major version release with authentication parameter updates and dependency version expansion.

## Upgrading

* The `key` parameter in the `Dispatcher` constructor is deprecated. Use `auth_key` instead.
* The `sign_secret` parameter is available and should be used for authentication. It will be soon required.
* The `components` property in `DispatchInfo` is deprecated. Use `target` instead.

## New Features

* `dry_run` status is now considered when merging dispatches. Dispatches with different `dry_run` values will no longer be merged, ensuring that dry-run and operational dispatches are handled by separate actors.
* Two new parameters were added to the `Dispatcher` constructor:
  * `sign_secret`: A secret key used for signing messages.
  * `auth_key`: An authentication key for the Dispatch API.
* `Dispatcher` now only fetches ongoing dispatches, excluding completed ones, to optimize performance and relevance.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
