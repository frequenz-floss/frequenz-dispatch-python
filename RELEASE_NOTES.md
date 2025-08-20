# Dispatch Highlevel Interface Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

* The `key` parameter in the `Dispatcher` constructor is now deprecated. Use `auth_key` instead. The `sign_secret` parameter is an additional optional parameter for signing.

## New Features

* Two new parameters were added to the `Dispatcher` constructor:
  * `sign_secret`: A secret key used for signing messages.
  * `auth_key`: An authentication key for the Dispatch API.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
