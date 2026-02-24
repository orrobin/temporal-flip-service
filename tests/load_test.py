import asyncio
import logging
import os
import random
import string
import time

import temporalio.client

from exercise import workflows

logger = logging.getLogger(__name__)

TEMPORAL_HOST = os.environ.get("TEMPORAL_HOST", "localhost:7233")

NUM_WORKFLOWS = 100
STRINGS_PER_WORKFLOW = 100
STRING_LENGTH = 8
PROGRESS_LOG_INTERVAL = 10


def generate_random_strings(count: int, length: int) -> list[str]:
    return [
        "".join(random.choices(string.ascii_lowercase, k=length)) for _ in range(count)
    ]


async def run_workflow(
    client: temporalio.client.Client,
    workflow_id: str,
    strings: list[str],
) -> tuple[str, list[str], list[str], int, int]:
    """Run a single workflow and return (id, input, output, flipped, ok_responses)."""
    handle = await client.start_workflow(
        workflows.Flip.run,
        workflows.Params(strings=strings),
        id=workflow_id,
        task_queue="default",
    )
    result = await handle.result()
    ok_responses = await handle.query(workflows.Flip.ok_responses)
    return workflow_id, strings, result.strings, len(result.strings), ok_responses


async def collect_results(
    tasks: list[asyncio.Task[tuple[str, list[str], list[str], int, int]]],
    start: float,
) -> tuple[int, int, int, int]:
    """Collect workflow results, logging per-workflow stats. Returns counters."""
    completed = 0
    failed = 0
    mismatched = 0
    total_ok_responses = 0

    for coro in asyncio.as_completed(tasks):
        try:
            wf_id, inputs, outputs, flipped, ok_responses = await coro
            completed += 1
            total_ok_responses += ok_responses

            logger.info(
                "[done] %s: strings flipped: %d | HTTP 200 responses: %d",
                wf_id,
                flipped,
                ok_responses,
            )

            if outputs != [s[::-1] for s in inputs]:
                mismatched += 1
                logger.warning("MISMATCH in %s", wf_id)

            if completed % PROGRESS_LOG_INTERVAL == 0:
                elapsed = time.monotonic() - start
                logger.info(
                    "%d/%d workflows completed [%.1fs]",
                    completed,
                    NUM_WORKFLOWS,
                    elapsed,
                )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            failed += 1
            logger.error("FAILED: %s", exc)

    return completed, failed, mismatched, total_ok_responses


async def main() -> None:
    """Run load test: launch many workflows concurrently and report results."""
    logging.basicConfig(level=logging.INFO)

    client = await temporalio.client.Client.connect(TEMPORAL_HOST)

    workflow_inputs: list[tuple[str, list[str]]] = [
        (f"load-test-{i}", generate_random_strings(STRINGS_PER_WORKFLOW, STRING_LENGTH))
        for i in range(NUM_WORKFLOWS)
    ]

    logger.info(
        "Starting %d workflows, each with %d strings...",
        NUM_WORKFLOWS,
        STRINGS_PER_WORKFLOW,
    )
    start = time.monotonic()

    tasks = [
        asyncio.create_task(run_workflow(client, wf_id, strings))
        for wf_id, strings in workflow_inputs
    ]

    completed, failed, mismatched, total_ok_responses = await collect_results(
        tasks, start
    )

    elapsed = time.monotonic() - start
    total_strings = NUM_WORKFLOWS * STRINGS_PER_WORKFLOW

    logger.info("--- Results ---")
    logger.info("Workflows:    %d completed, %d failed", completed, failed)
    logger.info("Mismatches:   %d", mismatched)
    logger.info("Strings:      %d total", total_strings)
    logger.info("HTTP 200s:    %d total", total_ok_responses)
    logger.info("Duration:     %.1fs", elapsed)
    logger.info("Throughput:   %.1f strings/s", total_strings / elapsed)


if __name__ == "__main__":
    asyncio.run(main())
