from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from typing import Any

import pytest
import temporalio.activity
import temporalio.client
import temporalio.testing
import temporalio.worker

from exercise.workflows.workflow import BatchResult, Flip, Params

# Mutable counter to track calls across activity invocations
_call_count: list[int] = [0]


@temporalio.activity.defn(name="flip_batch")
async def mock_flip_batch_always_succeeds(batch: list[str]) -> BatchResult:
    return BatchResult(strings=[s[::-1] for s in batch])


@temporalio.activity.defn(name="flip_batch")
async def mock_flip_batch_partial_then_succeeds(batch: list[str]) -> BatchResult:
    """First call returns '-' for the second string, second call succeeds."""
    _call_count[0] += 1
    if _call_count[0] == 1:
        results = ["-" if i == 1 else s[::-1] for i, s in enumerate(batch)]
        return BatchResult(strings=results)
    return BatchResult(strings=[s[::-1] for s in batch])


@asynccontextmanager
async def run_workflow(
    env: temporalio.testing.WorkflowEnvironment,
    strings: list[str],
    workflow_id: str,
    activity: Callable[..., Any] = mock_flip_batch_always_succeeds,
) -> AsyncGenerator[temporalio.client.WorkflowHandle, None]:
    """Run a Flip workflow and yield the handle while the worker is alive."""
    async with temporalio.worker.Worker(
        env.client,
        task_queue="test",
        workflows=[Flip],
        activities=[activity],
    ):
        handle = await env.client.start_workflow(
            Flip.run,
            Params(strings=strings),
            id=workflow_id,
            task_queue="test",
        )
        await handle.result()
        yield handle


TWELVE_STRINGS = [
    "alpha",
    "bravo",
    "charlie",
    "delta",
    "echo",
    "foxtrot",
    "golf",
    "hotel",
    "india",
    "juliet",
    "kilo",
    "lima",
]


@pytest.mark.asyncio
async def test_workflow_single_batch(
    workflow_env: temporalio.testing.WorkflowEnvironment,
) -> None:
    async with run_workflow(workflow_env, ["hello", "world"], "test-single-batch") as h:
        result = await h.result()
        assert result.strings == ["olleh", "dlrow"]


@pytest.mark.asyncio
async def test_workflow_multi_batch(
    workflow_env: temporalio.testing.WorkflowEnvironment,
) -> None:
    """12 strings require 2 batches (10 + 2)."""
    async with run_workflow(workflow_env, TWELVE_STRINGS, "test-multi-batch") as h:
        result = await h.result()
        assert result.strings == [s[::-1] for s in TWELVE_STRINGS]


@pytest.mark.asyncio
async def test_workflow_retries_partial_failures(
    workflow_env: temporalio.testing.WorkflowEnvironment,
) -> None:
    _call_count[0] = 0
    async with run_workflow(
        workflow_env,
        ["hello", "world"],
        "test-partial-retry",
        activity=mock_flip_batch_partial_then_succeeds,
    ) as h:
        result = await h.result()
        assert result.strings == ["olleh", "dlrow"]


@pytest.mark.asyncio
async def test_workflow_queries(
    workflow_env: temporalio.testing.WorkflowEnvironment,
) -> None:
    async with run_workflow(workflow_env, ["hello"], "test-queries") as h:
        assert await h.query(Flip.strings_flipped) == 1
        assert await h.query(Flip.ok_responses) == 1


@pytest.mark.asyncio
async def test_workflow_queries_multi_batch(
    workflow_env: temporalio.testing.WorkflowEnvironment,
) -> None:
    """12 strings across 2 batches: strings_flipped=12, ok_responses=2."""
    async with run_workflow(
        workflow_env, TWELVE_STRINGS, "test-queries-multi-batch"
    ) as h:
        assert await h.query(Flip.strings_flipped) == 12
        assert await h.query(Flip.ok_responses) == 2


@pytest.mark.asyncio
async def test_workflow_queries_with_partial_failures(
    workflow_env: temporalio.testing.WorkflowEnvironment,
) -> None:
    """Partial failure adds an extra round, increasing ok_responses."""
    _call_count[0] = 0
    async with run_workflow(
        workflow_env,
        ["hello", "world"],
        "test-queries-partial",
        activity=mock_flip_batch_partial_then_succeeds,
    ) as h:
        # Round 1: 1 string flipped, 1 failed ("-") -> ok_responses=1
        # Round 2: 1 remaining string flipped -> ok_responses=2
        assert await h.query(Flip.strings_flipped) == 2
        assert await h.query(Flip.ok_responses) == 2


@pytest.mark.asyncio
async def test_workflow_empty_input(
    workflow_env: temporalio.testing.WorkflowEnvironment,
) -> None:
    async with run_workflow(workflow_env, [], "test-empty") as h:
        result = await h.result()
        assert result.strings == []


@pytest.mark.asyncio
async def test_workflow_preserves_order(
    workflow_env: temporalio.testing.WorkflowEnvironment,
) -> None:
    """Verify results match original input order even with multiple batches."""
    strings = [
        "abc",
        "def",
        "ghi",
        "jkl",
        "mno",
        "pqr",
        "stu",
        "vwx",
        "yza",
        "bcd",
        "efg",
    ]
    async with run_workflow(workflow_env, strings, "test-order") as h:
        result = await h.result()
        assert result.strings == [s[::-1] for s in strings]


@pytest.mark.asyncio
async def test_workflow_rejects_invalid_strings(
    workflow_env: temporalio.testing.WorkflowEnvironment,
) -> None:
    """Strings with non-lowercase chars should fail fast."""
    async with temporalio.worker.Worker(
        workflow_env.client,
        task_queue="test",
        workflows=[Flip],
        activities=[mock_flip_batch_always_succeeds],
    ):
        with pytest.raises(temporalio.client.WorkflowFailureError):
            await workflow_env.client.execute_workflow(
                Flip.run,
                Params(strings=["Hello", "world"]),
                id="test-invalid",
                task_queue="test",
            )
