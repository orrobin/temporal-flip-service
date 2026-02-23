import random
from dataclasses import dataclass, field

import temporalio.client
import uvicorn
from fastapi import FastAPI
from pydantic import RootModel

from exercise import tmprl

app = FastAPI(title="Flip API")


@dataclass
class _ClientHolder:
    client: temporalio.client.Client | None = field(default=None)


_holder = _ClientHolder()


async def get_temporal_client() -> temporalio.client.Client:
    if _holder.client is None:
        _holder.client = await temporalio.client.Client.connect("localhost:7233")
    return _holder.client


@app.post("/flip")
async def flip(request: RootModel[list[str]]) -> RootModel[list[str]]:
    """Flip strings via the Temporal workflow."""
    client = await get_temporal_client()
    result = await client.execute_workflow(
        tmprl.Flip.run,
        tmprl.Params(strings=request.root),
        id=f"flip-api-{random.randint(0, 1_000_000)}",
        task_queue="default",
    )
    return RootModel[list[str]](result.strings)


if __name__ == "__main__":
    uvicorn.run("exercise.api:app", host="127.0.0.1", port=8080, reload=True)
