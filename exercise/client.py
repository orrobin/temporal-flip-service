import asyncio
import contextlib
import logging
import sys
import uuid

import temporalio.client
import temporalio.service

from exercise import workflows
from exercise.config import TASK_QUEUE, TEMPORAL_HOST

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 2


async def poll_progress(handle: temporalio.client.WorkflowHandle) -> None:
    """Query the workflow for progress and print it periodically."""
    while True:
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        try:
            flipped = await handle.query(workflows.Flip.strings_flipped)
            ok_responses = await handle.query(workflows.Flip.ok_responses)
            logger.debug(
                "strings flipped: %d | HTTP 200 responses: %d",
                flipped,
                ok_responses,
            )
        except (
            temporalio.client.WorkflowQueryFailedError,
            temporalio.service.RPCError,
        ):
            # Workflow may have completed between iterations
            break


async def flip(to_flip: list[str]) -> list[str]:
    """Start the Flip workflow, poll for progress, and return results."""
    try:
        client = await temporalio.client.Client.connect(
            target_host=TEMPORAL_HOST,
        )
    except OSError:
        logger.exception("Failed to connect to Temporal at %s", TEMPORAL_HOST)
        raise

    handle = await client.start_workflow(
        workflows.Flip.run,
        workflows.Params(strings=to_flip),
        id=f"flip-{uuid.uuid4().hex[:8]}",
        task_queue=TASK_QUEUE,
    )
    logger.info("Started workflow %s", handle.id)

    poll_task = asyncio.create_task(poll_progress(handle))

    try:
        result: workflows.Result = await handle.result()
    except Exception:
        logger.exception("Workflow %s failed", handle.id)
        raise
    finally:
        poll_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await poll_task

    logger.info(
        "Workflow %s completed: %d strings flipped, %d HTTP 200 responses",
        handle.id,
        len(result.strings),
        await handle.query(workflows.Flip.ok_responses),
    )

    return result.strings


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) < 2:
        print("Usage: python client.py <string> [<string> ...]")
        sys.exit(1)

    flipped_strings = asyncio.run(flip(sys.argv[1:]))

    print(" ".join(f'"{s}"' for s in flipped_strings))
