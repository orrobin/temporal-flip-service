import asyncio
import logging
from collections.abc import Callable
from typing import Any

import temporalio.client
import temporalio.worker
import temporalio.workflow

from exercise import tmprl

interrupt_event = asyncio.Event()

ACTIVITIES: list[Callable[..., Any]] = [tmprl.flip_batch]
WORKFLOWS = [tmprl.Flip]


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    client = await temporalio.client.Client.connect("localhost:7233")

    async with temporalio.worker.Worker(
        client,
        task_queue="default",
        workflows=WORKFLOWS,
        activities=ACTIVITIES,
    ):
        print("Worker started, ctrl+c to exit")
        await interrupt_event.wait()
        print("Shutting down")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        interrupt_event.set()
        loop.run_until_complete(loop.shutdown_asyncgens())
