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
provides channels for receiving dispatch lifecycle events and running status 
updates, allowing you to build reactive applications that respond to dispatch 
state changes.

## Supported Platforms

The following platforms are officially supported (tested):

- **Python:** 3.11
- **Operating System:** Ubuntu Linux 20.04
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
    "frequenz-dispatch >= 0.10.1, < 0.11",
]
```

> [!NOTE]
> We recommend pinning the dependency to the latest version for programs,
> like `"frequenz-dispatch == 0.10.1"`, and specifying a version range
> spanning one major version for libraries, like
> `"frequenz-dispatch >= 0.10.1, < 0.11"`. We follow [semver](https://semver.org/).

## Quick Start

The `frequenz-dispatch` library provides a high-level interface to interact
with the dispatch API. Here's a minimal example to get you started:

```python
import os
from frequenz.dispatch import Dispatcher

async def main():
    # Configure connection to dispatch API
    url = os.getenv("DISPATCH_API_URL", "grpc://your-dispatch-url.com")
    key = os.getenv("DISPATCH_API_KEY", "your-api-key")
    microgrid_id = 1
    
    # Create and use the dispatcher
    async with Dispatcher(
        microgrid_id=microgrid_id,
        server_url=url,
        key=key,
    ) as dispatcher:
        # Your dispatch logic here
        print("Dispatcher ready!")
```

The [`Dispatcher` class][dispatcher-class], the main entry point for the API,
provides two channels:

* [Lifecycle events][lifecycle-events]: A channel that sends a message whenever
  a [Dispatch][frequenz.dispatch.Dispatch] is created, updated or deleted.
* [Running status change][running-status-change]: Sends a dispatch message
  whenever a dispatch is ready to be executed according to the schedule or the
  running status of the dispatch changed in a way that could potentially
  require the actor to start, stop or reconfigure itself.

### Example using the running status change channel

```python
import os
from unittest.mock import MagicMock
from datetime import timedelta

from frequenz.dispatch import Dispatcher, DispatchInfo, MergeByType

async def create_actor(
    dispatch: DispatchInfo, receiver: Receiver[DispatchInfo]
) -> Actor:
    return MagicMock(dispatch=dispatch, receiver=receiver)

async def run():
    url = os.getenv(
        "DISPATCH_API_URL", "grpc://dispatch.url.goes.here.example.com"
    )
    key = os.getenv("DISPATCH_API_KEY", "some-key")

    microgrid_id = 1

    async with Dispatcher(
        microgrid_id=microgrid_id,
        server_url=url,
        key=key,
    ) as dispatcher:
        await dispatcher.start_managing(
            dispatch_type="EXAMPLE_TYPE",
            actor_factory=create_actor,
            merge_strategy=MergeByType(),
            retry_interval=timedelta(seconds=10)
        )

        await dispatcher
```

[dispatcher-class]: https://frequenz-floss.github.io/frequenz-dispatch-python/latest/reference/frequenz/dispatch/#frequenz.dispatch.Dispatcher
[lifecycle-events]: https://frequenz-floss.github.io/frequenz-dispatch-python/latest/reference/frequenz/dispatch/#frequenz.dispatch.Dispatcher.lifecycle_events
[running-status-change]: https://frequenz-floss.github.io/frequenz-dispatch-python/latest/reference/frequenz/dispatch/#frequenz.dispatch.Dispatcher.running_status_change

## Documentation

For complete API documentation, examples, and advanced usage patterns, see 
[the documentation](https://frequenz-floss.github.io/frequenz-dispatch-python/latest/reference/frequenz/dispatch).

## Contributing

If you want to know how to build this project and contribute to it, please
check out the [Contributing Guide](CONTRIBUTING.md).
