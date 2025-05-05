# Dispatch Highlevel Interface Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

* Fixes reconnecting after connection loss for streams
* Fixed an issue in the `Dispatcher` class where the client connection was not properly disconnected during cleanup, potentially causing unclosed socket errors.
