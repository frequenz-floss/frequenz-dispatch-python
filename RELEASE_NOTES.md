# Dispatch Highlevel Interface Release Notes

## New Features

* This release now supports the sdk up to rc2000.
* Less logs are now on `INFO` level, and more on `DEBUG` level, making the output less verbose.
* Changed the type of `DispatchInfo.components` from `list[int] | list[ComponentCategory]` to `list[ComponentId] | list[ComponentCategory]`, where `ComponentId` is imported from `frequenz.client.microgrid`.

