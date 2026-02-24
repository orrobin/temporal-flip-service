"""Shared configuration constants for the exercise package."""

import os

TEMPORAL_HOST = os.environ.get("TEMPORAL_HOST", "localhost:7233")
TASK_QUEUE = "default"
