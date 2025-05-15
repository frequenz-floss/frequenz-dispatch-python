# Dispatch Highlevel Interface

[![Build Status](https://github.com/frequenz-floss/frequenz-dispatch-python/actions/workflows/ci.yaml/badge.svg)](https://github.com/frequenz-floss/frequenz-dispatch-python/actions/workflows/ci.yaml)
[![PyPI Package](https://img.shields.io/pypi/v/frequenz-dispatch)](https://pypi.org/project/frequenz-dispatch/)
[![Docs](https://img.shields.io/badge/docs-latest-informational)](https://frequenz-floss.github.io/frequenz-dispatch-python/)

## Introduction

A highlevel interface for the dispatch API.

See [the documentation](https://frequenz-floss.github.io/frequenz-dispatch-python/v0.1/reference/frequenz/dispatch) for more information.

## Usage

The [`Dispatcher` class](https://frequenz-floss.github.io/frequenz-dispatch-python/v0.1/reference/frequenz/dispatch/#frequenz.dispatch.Dispatcher), the main entry point for the API, provides two channels:

* [Lifecycle events](https://frequenz-floss.github.io/frequenz-dispatch-python/v0.1/reference/frequenz/dispatch/#frequenz.dispatch.Dispatcher.lifecycle_events): A channel that sends a message whenever a [Dispatch][frequenz.dispatch.Dispatch] is created, updated or deleted.
* [Running status change](https://frequenz-floss.github.io/frequenz-dispatch-python/v0.1/reference/frequenz/dispatch/#frequenz.dispatch.Dispatcher.running_status_change): Sends a dispatch message whenever a dispatch is ready to be executed according to the schedule or the running status of the dispatch changed in a way that could potentially require the actor to start, stop or reconfigure itself.

### Example using the running status change channel

```python
import os
from unittest.mock import MagicMock
from datetime import timedelta

from frequenz.dispatch import Dispatcher, DispatchInfo, MergeByType

async def create_actor(dispatch: DispatchInfo, receiver: Receiver[DispatchInfo]) -> Actor:
    return MagicMock(dispatch=dispatch, receiver=receiver)

async def run():
    url = os.getenv("DISPATCH_API_URL", "grpc://dispatch.url.goes.here.example.com")
    key  = os.getenv("DISPATCH_API_KEY", "some-key")

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

## Supported Platforms

The following platforms are officially supported (tested):

- **Python:** 3.11
- **Operating System:** Ubuntu Linux 20.04
- **Architectures:** amd64, arm64

## Contributing

If you want to know how to build this project and contribute to it, please
check out the [Contributing Guide](CONTRIBUTING.md).
