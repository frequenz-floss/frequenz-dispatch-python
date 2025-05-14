# Dispatch Highlevel Interface Release Notes

## Upgrading

This is a breaking release that requires you to use the new URL for the dispatch service:

 * Staging: `grpc://dispatch.eu-1.staging.api.frequenz.com:443`
 * Production: `grpc://dispatch.eu-1.prod.api.frequenz.com:443`

## Bug Fixes

* Fix that a user might see invalid values for dispatches without `end_time`.
