# Dispatch Highlevel Interface Release Notes

## Summary

This release introduces a more flexible and powerful mechanism for managing dispatch events with new strategies for merging intervals, enhanced customization options, and better overall alignment with evolving SDK dependencies. It also simplifies actor initialization while maintaining robust support for diverse dispatch scenarios.

## Upgrading

A new simplified way to manage actors has been introduced:

Change your code from:
```python
dispatcher = Dispatcher(
    microgrid_id=microgrid_id,
    server_url=url,
    key=key
)
dispatcher.start()

status_receiver = dispatcher.new_running_state_event_receiver("EXAMPLE_TYPE")

managing_actor = ActorDispatcher(
    actor_factory=MyActor.new_with_dispatch,
    running_status_receiver=status_receiver,
)

await run(managing_actor)
```

to

```python
async with Dispatcher(
    microgrid_id=microgrid_id,
    server_url=url,
    key=key
) as dispatcher:
    await dispatcher.start_managing(
        dispatch_type="EXAMPLE_TYPE",
        actor_factory=MyActor.new_with_dispatch, # now async factory!
        merge_strategy=MergeByType,
    )
    await dispatcher
```

Further changes:

* `Dispatcher.start` is no longer `async`. Remove `await` when calling it.
* Two properties have been replaced by methods that require a type as parameter.
    * `Dispatcher.lifecycle_events` has been replaced by the method `Dispatcher.new_lifecycle_events_receiver(self, dispatch_type: str)`.
    * `Dispatcher.running_status_change` has been replaced by the method `Dispatcher.new_running_state_event_receiver(self, dispatch_type: str, merge_strategy: MergeStrategy)`.
* The managing actor constructor no longer requires the `dispatch_type` parameter. Instead you're expected to pass the type to the new_receiver function.
* The `DispatchManagingActor` class has been renamed to `DispatchActorsService`.
    * It's interface has been simplified and now only requires an actor factory and a running status receiver.
    * It only starts/stops a single actor at a time now instead of a set of actors.
    * Refer to the updated [usage example](https://frequenz-floss.github.io/frequenz-dispatch-python/latest/reference/frequenz/dispatch/#frequenz.dispatch.DispatchActorsService) for more information.
* `DispatchUpdate` was renamed to `DispatchInfo`.


## New Features

* A new feature "merge strategy" (`MergeByType`, `MergeByTypeTarget`) has been added to the `Dispatcher.new_running_state_event_receiver` method. Using it, you can automatically merge consecutive and overlapping dispatch start/stop events of the same type. E.g. dispatch `A` starting at 10:10 and ending at 10:30 and dispatch `B` starts at 10:30 until 11:00, with the feature enabled this would in total trigger one start event, one reconfigure event at 10:30 and one stop event at 11:00.
* The SDK dependency was widened to allow versions up to (excluding) v1.0.0-rc1800.
* Actor management with dispatches has been simplified:
    * `Dispatcher.start_managing(dispatch_type, actor_factory, merge_strategy, retry_interval)` to manage your actor for the given type and merge strategy. All you need provide is an actor factory.
    * `Dispatcher.stop_managing(dispatch_type)` to stop dispatching for the given type.
    * `Dispatcher.is_managed(dispatch_type)` to check if dispatching is active for the given type.
    * Dispatches that failed to start will now be retried after a delay.
* A new method `Dispatcher.wait_for_initialization()` has been added to wait for all actors to be initialized.
* When using `async with Dispatcher(..) as dispatcher`, the dispatcher will first wait for the dispatch service to be initialized before entering the block.
