# Dispatch Highlevel Interface Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

The `frequenz.disptach.TargetComponents` tye was removed, use `frequenz.client.dispatch.TargetComponents` instead.

## New Features

* The dispatcher offers two new parameters to control the client's call and stream timeout:
  - `call_timeout`: The maximum time to wait for a response from the client.
  - `stream_timeout`: The maximum time to wait before restarting a stream.
* While the dispatch stream restarts we refresh our dispatch cache as well, to ensure we didn't miss any updates.

## Bug Fixes

* Fixed that dispatches are never retried on failure, but instead an infinite loop of retry logs is triggered.
