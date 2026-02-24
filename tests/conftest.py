from collections.abc import AsyncGenerator, Generator
from contextlib import contextmanager
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import temporalio.testing


@pytest.fixture
def successful_response() -> list[str]:
    return ["olleh", "dlrow"]


@pytest.fixture
def partial_failure_response() -> list[str]:
    return ["olleh", "-"]


@contextmanager
def mock_http_client(response: httpx.Response) -> Generator[AsyncMock, None, None]:
    """Patch httpx.AsyncClient to return a predetermined response."""
    with patch("exercise.workflows.workflow.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.post.return_value = response
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=None)
        yield mock_client


@pytest.fixture
async def workflow_env() -> (
    AsyncGenerator[temporalio.testing.WorkflowEnvironment, None]
):
    """Provide a local Temporal test environment for workflow tests."""
    async with await temporalio.testing.WorkflowEnvironment.start_local() as env:
        yield env
