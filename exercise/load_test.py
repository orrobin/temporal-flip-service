import asyncio
import random
import string
import time

import temporalio.client

from exercise import tmprl

NUM_WORKFLOWS = 100
STRINGS_PER_WORKFLOW = 100
STRING_LENGTH = 8


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
        tmprl.Flip.run,
        tmprl.Params(strings=strings),
        id=workflow_id,
        task_queue="default",
    )
    result = await handle.result()
    ok_responses = await handle.query(tmprl.Flip.ok_responses)
    return workflow_id, strings, result.strings, len(result.strings), ok_responses


async def main() -> None:
    client = await temporalio.client.Client.connect("localhost:7233")

    # Generate all inputs upfront
    workflows: list[tuple[str, list[str]]] = [
        (f"load-test-{i}", generate_random_strings(STRINGS_PER_WORKFLOW, STRING_LENGTH))
        for i in range(NUM_WORKFLOWS)
    ]

    print(
        f"Starting {NUM_WORKFLOWS} workflows, "
        f"each with {STRINGS_PER_WORKFLOW} strings..."
    )
    start = time.monotonic()

    # Launch all workflows concurrently
    tasks = [
        asyncio.create_task(run_workflow(client, wf_id, strings))
        for wf_id, strings in workflows
    ]

    completed = 0
    failed = 0
    mismatched = 0
    total_ok_responses = 0

    for coro in asyncio.as_completed(tasks):
        try:
            wf_id, inputs, outputs, flipped, ok_responses = await coro
            completed += 1
            total_ok_responses += ok_responses

            print(
                f"  [done] {wf_id}: strings flipped: {flipped} | "
                f"HTTP 200 responses: {ok_responses}"
            )

            # Verify correctness: each output should be the reverse of its input
            expected = [s[::-1] for s in inputs]
            if outputs != expected:
                mismatched += 1
                print(f"  MISMATCH in {wf_id}")

            if completed % 10 == 0:
                elapsed = time.monotonic() - start
                print(
                    f"  --- {completed}/{NUM_WORKFLOWS} "
                    f"workflows completed [{elapsed:.1f}s] ---"
                )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            failed += 1
            print(f"  FAILED: {exc}")

    elapsed = time.monotonic() - start
    total_strings = NUM_WORKFLOWS * STRINGS_PER_WORKFLOW

    print("\n--- Results ---")
    print(f"Workflows:    {completed} completed, {failed} failed")
    print(f"Mismatches:   {mismatched}")
    print(f"Strings:      {total_strings} total")
    print(f"HTTP 200s:    {total_ok_responses} total")
    print(f"Duration:     {elapsed:.1f}s")
    print(f"Throughput:   {total_strings / elapsed:.1f} strings/s")


if __name__ == "__main__":
    asyncio.run(main())
