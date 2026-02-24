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


@pytest.mark.asyncio
async def test_workflow_single_batch() -> None:
    async with await temporalio.testing.WorkflowEnvironment.start_local() as env:
        async with temporalio.worker.Worker(
            env.client,
            task_queue="test",
            workflows=[Flip],
            activities=[mock_flip_batch_always_succeeds],
        ):
            result = await env.client.execute_workflow(
                Flip.run,
                Params(strings=["hello", "world"]),
                id="test-single-batch",
                task_queue="test",
            )
            assert result.strings == ["olleh", "dlrow"]


@pytest.mark.asyncio
async def test_workflow_multi_batch() -> None:
    """12 strings require 2 batches (10 + 2)."""
    strings = [
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

    async with await temporalio.testing.WorkflowEnvironment.start_local() as env:
        async with temporalio.worker.Worker(
            env.client,
            task_queue="test",
            workflows=[Flip],
            activities=[mock_flip_batch_always_succeeds],
        ):
            result = await env.client.execute_workflow(
                Flip.run,
                Params(strings=strings),
                id="test-multi-batch",
                task_queue="test",
            )
            assert result.strings == [s[::-1] for s in strings]


@pytest.mark.asyncio
async def test_workflow_retries_partial_failures() -> None:
    _call_count[0] = 0

    async with await temporalio.testing.WorkflowEnvironment.start_local() as env:
        async with temporalio.worker.Worker(
            env.client,
            task_queue="test",
            workflows=[Flip],
            activities=[mock_flip_batch_partial_then_succeeds],
        ):
            result = await env.client.execute_workflow(
                Flip.run,
                Params(strings=["hello", "world"]),
                id="test-partial-retry",
                task_queue="test",
            )
            assert result.strings == ["olleh", "dlrow"]


@pytest.mark.asyncio
async def test_workflow_queries() -> None:
    async with await temporalio.testing.WorkflowEnvironment.start_local() as env:
        async with temporalio.worker.Worker(
            env.client,
            task_queue="test",
            workflows=[Flip],
            activities=[mock_flip_batch_always_succeeds],
        ):
            handle = await env.client.start_workflow(
                Flip.run,
                Params(strings=["hello"]),
                id="test-queries",
                task_queue="test",
            )
            await handle.result()

            flipped = await handle.query(Flip.strings_flipped)
            ok_responses = await handle.query(Flip.ok_responses)

            assert flipped == 1
            assert ok_responses == 1


@pytest.mark.asyncio
async def test_workflow_queries_multi_batch() -> None:
    """12 strings across 2 batches: strings_flipped=12, ok_responses=2."""
    strings = [
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

    async with await temporalio.testing.WorkflowEnvironment.start_local() as env:
        async with temporalio.worker.Worker(
            env.client,
            task_queue="test",
            workflows=[Flip],
            activities=[mock_flip_batch_always_succeeds],
        ):
            handle = await env.client.start_workflow(
                Flip.run,
                Params(strings=strings),
                id="test-queries-multi-batch",
                task_queue="test",
            )
            await handle.result()

            assert await handle.query(Flip.strings_flipped) == 12
            assert await handle.query(Flip.ok_responses) == 2


@pytest.mark.asyncio
async def test_workflow_queries_with_partial_failures() -> None:
    """Partial failure adds an extra round, increasing ok_responses."""
    _call_count[0] = 0

    async with await temporalio.testing.WorkflowEnvironment.start_local() as env:
        async with temporalio.worker.Worker(
            env.client,
            task_queue="test",
            workflows=[Flip],
            activities=[mock_flip_batch_partial_then_succeeds],
        ):
            handle = await env.client.start_workflow(
                Flip.run,
                Params(strings=["hello", "world"]),
                id="test-queries-partial",
                task_queue="test",
            )
            await handle.result()

            # Round 1: 1 string flipped, 1 failed ("-") -> ok_responses=1
            # Round 2: 1 remaining string flipped -> ok_responses=2
            assert await handle.query(Flip.strings_flipped) == 2
            assert await handle.query(Flip.ok_responses) == 2


@pytest.mark.asyncio
async def test_workflow_empty_input() -> None:
    async with await temporalio.testing.WorkflowEnvironment.start_local() as env:
        async with temporalio.worker.Worker(
            env.client,
            task_queue="test",
            workflows=[Flip],
            activities=[mock_flip_batch_always_succeeds],
        ):
            result = await env.client.execute_workflow(
                Flip.run,
                Params(strings=[]),
                id="test-empty",
                task_queue="test",
            )
            assert result.strings == []


@pytest.mark.asyncio
async def test_workflow_preserves_order() -> None:
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

    async with await temporalio.testing.WorkflowEnvironment.start_local() as env:
        async with temporalio.worker.Worker(
            env.client,
            task_queue="test",
            workflows=[Flip],
            activities=[mock_flip_batch_always_succeeds],
        ):
            result = await env.client.execute_workflow(
                Flip.run,
                Params(strings=strings),
                id="test-order",
                task_queue="test",
            )
            assert result.strings == [s[::-1] for s in strings]


@pytest.mark.asyncio
async def test_workflow_rejects_invalid_strings() -> None:
    """Strings with non-lowercase chars should fail fast."""
    async with await temporalio.testing.WorkflowEnvironment.start_local() as env:
        async with temporalio.worker.Worker(
            env.client,
            task_queue="test",
            workflows=[Flip],
            activities=[mock_flip_batch_always_succeeds],
        ):
            with pytest.raises(temporalio.client.WorkflowFailureError):
                await env.client.execute_workflow(
                    Flip.run,
                    Params(strings=["Hello", "world"]),
                    id="test-invalid",
                    task_queue="test",
                )
