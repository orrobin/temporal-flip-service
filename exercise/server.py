import asyncio
import random
import string

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.routing import APIRouter
from pydantic import RootModel


class FlipRouter(APIRouter):
    def __init__(self):
        super().__init__()
        self.add_api_route("/flip", self.flip, methods=["POST"])

    async def flip(self, request: RootModel[list[str]]) -> RootModel[list[str]]:
        """
        Flips the input strings. Returns error if the alphabet in the strings mismatches
        the current mode.
        Randomly fails (10% chance of immediate failure).
        Each string flip has a 90% chance of success; otherwise, returns `-`.
        Rejects requests with more than 10 strings.
        Rejects requests with strings containing characters outside the current alphabet mode.
        """

        to_flip = request.root

        # Immediate failure chance (10%)
        if random.random() < 0.1:
            raise HTTPException(
                status_code=500, detail="Random immediate failure occurred."
            )

        # Limit the number of strings to 10
        if len(to_flip) > 10:
            raise HTTPException(
                status_code=400, detail="Cannot process more than 10 strings at a time."
            )

        # Define the valid alphabets for the modes
        alphabet = set(string.ascii_lowercase)

        # Check if all strings belong to the current alphabet mode
        if not all(set(s).issubset(alphabet) for s in to_flip):
            raise HTTPException(
                status_code=400,
                detail="Strings contain characters outside the current alphabet mode.",
            )

        # Simulate processing time
        await asyncio.sleep(5)

        # Flip each string with 90% success, 10% remains the same
        to_flip = [s[::-1] if random.random() < 0.90 else "-" for s in to_flip]

        return RootModel[list[str]](to_flip)


app = FastAPI()
app.include_router(FlipRouter())

# Run the application
if __name__ == "__main__":
    uvicorn.run("server:app", host="127.0.0.1", port=8000, workers=1)
