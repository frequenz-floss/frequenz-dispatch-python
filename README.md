# Dispatch Highlevel Interface

[![Build Status](https://github.com/frequenz-floss/frequenz-dispatch-python/actions/workflows/ci.yaml/badge.svg)](https://github.com/frequenz-floss/frequenz-dispatch-python/actions/workflows/ci.yaml)
[![PyPI Package](https://img.shields.io/pypi/v/frequenz-dispatch)](https://pypi.org/project/frequenz-dispatch/)
[![Docs](https://img.shields.io/badge/docs-latest-informational)](https://frequenz-floss.github.io/frequenz-dispatch-python/)

## Introduction

The `frequenz-dispatch` library provides a high-level Python interface for
interacting with the Frequenz Dispatch API. This library enables you to
manage and monitor dispatch operations in microgrids, including lifecycle
events and running status changes of dispatch operations.

The main entry point is the [`Dispatcher`][dispatcher-class] class, which
provides methods for receiving dispatch lifecycle events and running status
updates, allowing you to build reactive applications that respond to dispatch
state changes.

## Supported Platforms

The following platforms are officially supported (tested):

- **Python:** 3.11, 3.13
- **Operating System:** Ubuntu Linux 24.04
- **Architectures:** amd64, arm64

## Installation

### Using pip

You can install the package from PyPI:

```bash
python3 -m pip install frequenz-dispatch
```

### Using pyproject.toml

Add the dependency to your `pyproject.toml` file:

```toml
[project]
dependencies = [
    "frequenz-dispatch >= 1.0.1, < 2",
]
```

> [!NOTE]
> We recommend pinning the dependency to the latest version for programs,
> like `"frequenz-dispatch == 1.0.1"`, and specifying a version range
> spanning one major version for libraries, like
> `"frequenz-dispatch >= 1.0.1, < 2"`. We follow [semver](https://semver.org/).

## Quick Start

Here's a minimal example to get you started with lifecycle events:

```python
import asyncio
import os

from frequenz.dispatch import Created, Deleted, Dispatcher, Updated

async def main() -> None:
    url = os.getenv("DISPATCH_API_URL", "grpc://localhost:50051")
    auth_key = os.getenv("DISPATCH_API_AUTH_KEY", "my-api-key")
    sign_secret = os.getenv("DISPATCH_API_SIGN_SECRET")
    microgrid_id = 1

    async with Dispatcher(
        microgrid_id=microgrid_id,
        server_url=url,
        auth_key=auth_key,
        sign_secret=sign_secret,
    ) as dispatcher:
        events_receiver = dispatcher.new_lifecycle_events_receiver("MY_TYPE")

        async for event in events_receiver:
            match event:
                case Created(dispatch):
                    print(f"Created: {dispatch}")
                case Updated(dispatch):
                    print(f"Updated: {dispatch}")
                case Deleted(dispatch):
                    print(f"Deleted: {dispatch}")

asyncio.run(main())
```

The [`Dispatcher` class][dispatcher-class] provides two receiver methods:

* [`new_lifecycle_events_receiver()`][lifecycle-events]: Returns a receiver
  that sends a message whenever a Dispatch is created, updated or deleted.
* [`new_running_state_event_receiver()`][running-status-change]: Returns a
  receiver that sends a dispatch message whenever a dispatch is ready to be
  executed according to the schedule or the running status of the dispatch
  changed in a way that could potentially require the actor to start, stop or
  reconfigure itself.

### Example managing actors with dispatch events

```python
import os
from datetime import timedelta
from unittest.mock import MagicMock

from frequenz.channels import Receiver
from frequenz.sdk.actor import Actor

from frequenz.dispatch import Dispatcher, DispatchInfo, MergeByType

async def create_actor(
    dispatch: DispatchInfo, receiver: Receiver[DispatchInfo]
) -> Actor:
    return MagicMock(dispatch=dispatch, receiver=receiver)

async def run() -> None:
    url = os.getenv(
        "DISPATCH_API_URL", "grpc://dispatch.api.example.com:50051"
    )
    auth_key = os.getenv("DISPATCH_API_AUTH_KEY", "my-api-key")
    sign_secret = os.getenv("DISPATCH_API_SIGN_SECRET")

    microgrid_id = 1

    async with Dispatcher(
        microgrid_id=microgrid_id,
        server_url=url,
        auth_key=auth_key,
        sign_secret=sign_secret,
    ) as dispatcher:
        await dispatcher.start_managing(
            dispatch_type="EXAMPLE_TYPE",
            actor_factory=create_actor,
            merge_strategy=MergeByType(),
            retry_interval=timedelta(seconds=10)
        )

        await dispatcher
```

## Documentation

For complete API documentation, examples, and advanced usage patterns, see
[the documentation][docs].

[dispatcher-class]: https://frequenz-floss.github.io/frequenz-dispatch-python/latest/reference/frequenz/dispatch/#frequenz.dispatch.Dispatcher
[lifecycle-events]: https://frequenz-floss.github.io/frequenz-dispatch-python/latest/reference/frequenz/dispatch/#frequenz.dispatch.Dispatcher.new_lifecycle_events_receiver
[running-status-change]: https://frequenz-floss.github.io/frequenz-dispatch-python/latest/reference/frequenz/dispatch/#frequenz.dispatch.Dispatcher.new_running_state_event_receiver
[docs]: https://frequenz-floss.github.io/frequenz-dispatch-python/latest/

## Contributing

If you want to know how to build this project and contribute to it, please
check out the [Contributing Guide](CONTRIBUTING.md).
