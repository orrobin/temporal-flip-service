import asyncio
import os
import random
import sys

import temporalio.client

from exercise import workflows

TEMPORAL_HOST = os.environ.get("TEMPORAL_HOST", "localhost:7233")

POLL_INTERVAL_SECONDS = 2


async def poll_progress(handle: temporalio.client.WorkflowHandle) -> None:
    """Query the workflow for progress and print it periodically."""
    while True:
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        try:
            flipped = await handle.query(workflows.Flip.strings_flipped)
            ok_responses = await handle.query(workflows.Flip.ok_responses)
            print(
                f"[progress] strings flipped: {flipped} | "
                f"HTTP 200 responses: {ok_responses}"
            )
        except temporalio.client.WorkflowQueryFailedError:
            break
        except Exception:  # pylint: disable=broad-exception-caught
            # Workflow may have completed between iterations
            break


async def flip(to_flip: list[str]) -> list[str]:
    client = await temporalio.client.Client.connect(
        target_host=TEMPORAL_HOST,
    )

    handle = await client.start_workflow(
        workflows.Flip.run,
        workflows.Params(strings=to_flip),
        id=f"flip-{random.randint(0, 100000)}",
        task_queue="default",
    )

    poll_task = asyncio.create_task(poll_progress(handle))

    try:
        result: workflows.Result = await handle.result()
    finally:
        poll_task.cancel()
        try:
            await poll_task
        except asyncio.CancelledError:
            pass

    print(
        f"[done] strings flipped: {len(result.strings)} | "
        f"HTTP 200 responses: {await handle.query(workflows.Flip.ok_responses)}"
    )

    return result.strings


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python client.py <string> [<string> ...]")
        sys.exit(1)

    flipped_strings = asyncio.run(flip(sys.argv[1:]))

    print(" ".join(f'"{s}"' for s in flipped_strings))
