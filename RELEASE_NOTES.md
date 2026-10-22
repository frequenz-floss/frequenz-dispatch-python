# Dispatch Highlevel Interface Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

- Added opt-in support for reusing a continuation-capable actor across exactly adjacent, compatible dispatches. Callers must provide a continuation predicate; ambiguous or unsupported handovers retain the normal stop/start behavior.

## Bug Fixes

- Prevented scheduler crashes when coincident events produced a non-positive timer interval.
