import logging
import os
import random
from dataclasses import dataclass, field

import temporalio.client
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import RootModel

from exercise import workflows

logger = logging.getLogger(__name__)

TEMPORAL_HOST = os.environ.get("TEMPORAL_HOST", "localhost:7233")

app = FastAPI(title="Flip API")


@dataclass
class _ClientHolder:
    """Holds a cached Temporal client for reuse across requests."""

    client: temporalio.client.Client | None = field(default=None)


_holder = _ClientHolder()


async def get_temporal_client() -> temporalio.client.Client:
    """Return a cached Temporal client, connecting on first use."""
    if _holder.client is None:
        try:
            _holder.client = await temporalio.client.Client.connect(TEMPORAL_HOST)
            logger.info("Connected to Temporal at %s", TEMPORAL_HOST)
        except Exception:
            logger.exception("Failed to connect to Temporal at %s", TEMPORAL_HOST)
            raise
    return _holder.client


@app.post("/flip")
async def flip(request: RootModel[list[str]]) -> RootModel[list[str]]:
    """Flip strings via the Temporal workflow."""
    try:
        client = await get_temporal_client()
    except Exception:
        raise HTTPException(
            status_code=503, detail="Temporal service unavailable"
        ) from None

    try:
        result = await client.execute_workflow(
            workflows.Flip.run,
            workflows.Params(strings=request.root),
            id=f"flip-api-{random.randint(0, 1_000_000)}",
            task_queue="default",
        )
    except temporalio.client.WorkflowFailureError as exc:
        logger.error("Workflow failed: %s", exc)
        raise HTTPException(status_code=422, detail=f"Workflow failed: {exc}") from exc
    except Exception:
        logger.exception("Unexpected error executing workflow")
        raise HTTPException(status_code=500, detail="Internal server error") from None

    logger.info("Flipped %d strings", len(result.strings))
    return RootModel[list[str]](result.strings)


if __name__ == "__main__":
    uvicorn.run("exercise.api:app", host="127.0.0.1", port=8080, reload=True)
