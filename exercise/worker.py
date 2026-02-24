import asyncio
import logging
from collections.abc import Callable
from typing import Any

import temporalio.client
import temporalio.worker
import temporalio.workflow

from exercise import workflows
from exercise.config import TASK_QUEUE, TEMPORAL_HOST

logger = logging.getLogger(__name__)

interrupt_event = asyncio.Event()

ACTIVITIES: list[Callable[..., Any]] = [workflows.flip_batch]
WORKFLOWS = [workflows.Flip]


async def main() -> None:
    """Connect to Temporal and run the worker until interrupted."""
    logging.basicConfig(level=logging.INFO)

    try:
        client = await temporalio.client.Client.connect(TEMPORAL_HOST)
    except OSError:
        logger.exception("Failed to connect to Temporal at %s", TEMPORAL_HOST)
        raise

    logger.info("Connected to Temporal at %s", TEMPORAL_HOST)

    async with temporalio.worker.Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=WORKFLOWS,
        activities=ACTIVITIES,
    ):
        logger.info("Worker started, ctrl+c to exit")
        await interrupt_event.wait()
        logger.warning("Shutting down")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        interrupt_event.set()
        loop.run_until_complete(loop.shutdown_asyncgens())
