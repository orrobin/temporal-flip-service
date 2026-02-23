import asyncio
import dataclasses
import logging
from datetime import timedelta

import httpx
import temporalio.activity
import temporalio.common
import temporalio.exceptions
import temporalio.workflow

logger = logging.getLogger(__name__)

FLIP_SERVER_URL = "http://127.0.0.1:8000/flip"
MAX_BATCH_SIZE = 10
ACTIVITY_START_TO_CLOSE_TIMEOUT = timedelta(seconds=30)

PARTIAL_FAILURE_MARKER = "-"


@dataclasses.dataclass
class Params:
    strings: list[str]


@dataclasses.dataclass
class Result:
    strings: list[str]


@dataclasses.dataclass
class BatchResult:
    strings: list[str]


@temporalio.activity.defn
async def flip_batch(batch: list[str]) -> BatchResult:
    """Call the flip server with a batch of strings (max 10)."""
    logger.info("Sending batch of %d strings to flip server", len(batch))
    async with httpx.AsyncClient() as client:
        response = await client.post(
            FLIP_SERVER_URL,
            json=batch,
            timeout=15.0,
        )

    if response.status_code == 400:
        detail = response.json().get("detail", "Bad request")
        raise temporalio.exceptions.ApplicationError(
            f"Flip server rejected request: {detail}",
            non_retryable=True,
        )

    response.raise_for_status()

    result: list[str] = response.json()
    logger.info("Received %d results from flip server", len(result))
    return BatchResult(strings=result)


@temporalio.workflow.defn(name="Flip", sandboxed=False)
class Flip:
    def __init__(self) -> None:
        self._strings_flipped: int = 0
        self._ok_responses: int = 0

    @temporalio.workflow.query
    def strings_flipped(self) -> int:
        """Number of individual strings successfully flipped so far."""
        return self._strings_flipped

    @temporalio.workflow.query
    def ok_responses(self) -> int:
        """Number of HTTP 200 OK responses received from the flip server."""
        return self._ok_responses

    @temporalio.workflow.run
    async def run(self, params: Params) -> Result:
        total = len(params.strings)
        results: list[str | None] = [None] * total
        pending: list[tuple[int, str]] = list(enumerate(params.strings))
        round_number = 0

        retry_policy = temporalio.common.RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=10),
            maximum_attempts=0,  # unlimited
        )

        while pending:
            round_number += 1
            temporalio.workflow.logger.info(
                "Round %d: %d/%d strings pending",
                round_number,
                len(pending),
                total,
            )
            pending = await self._execute_round(pending, results, retry_policy)

        return Result(strings=[s for s in results if s is not None])

    async def _execute_round(
        self,
        pending: list[tuple[int, str]],
        results: list[str | None],
        retry_policy: temporalio.common.RetryPolicy,
    ) -> list[tuple[int, str]]:
        """Execute one round of parallel batch flips, returning still-pending items."""
        batches: list[list[tuple[int, str]]] = [
            pending[i : i + MAX_BATCH_SIZE]
            for i in range(0, len(pending), MAX_BATCH_SIZE)
        ]

        batch_results: list[BatchResult] = await asyncio.gather(
            *[
                temporalio.workflow.execute_activity(
                    flip_batch,
                    [s for _, s in batch],
                    start_to_close_timeout=ACTIVITY_START_TO_CLOSE_TIMEOUT,
                    retry_policy=retry_policy,
                )
                for batch in batches
            ]
        )

        next_pending: list[tuple[int, str]] = []
        for batch, batch_result in zip(batches, batch_results):
            self._ok_responses += 1
            for (orig_idx, orig_str), flipped in zip(batch, batch_result.strings):
                if flipped == PARTIAL_FAILURE_MARKER:
                    next_pending.append((orig_idx, orig_str))
                else:
                    results[orig_idx] = flipped
                    self._strings_flipped += 1

        return next_pending
