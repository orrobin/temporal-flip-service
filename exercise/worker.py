import asyncio
import logging
import warnings

import temporalio.client
import temporalio.worker
import temporalio.workflow

from exercise import tmprl

interrupt_event = asyncio.Event()

ACTIVITIES = []  # TODO add the activities here
WORKFLOWS = [tmprl.Flip]


async def main():
    # Uncomment the line below to see logging
    logging.basicConfig(level=logging.INFO)

    # Start client
    client = await temporalio.client.Client.connect("localhost:7233")
    if len(ACTIVITIES) == 0:
        warnings.warn(
            "No activities specified, worker will not be able to execute any non-workflow tasks"
        )
    # Run a worker for the workflow
    async with temporalio.worker.Worker(
        client,
        task_queue="default",
        workflows=WORKFLOWS,
        activities=ACTIVITIES,
    ):
        # Wait until interrupted
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
