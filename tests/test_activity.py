from unittest.mock import AsyncMock, patch

import httpx
import pytest
import temporalio.exceptions

from exercise.tmprl.workflow import BatchResult, flip_batch


@pytest.mark.asyncio
async def test_flip_batch_success() -> None:
    mock_response = httpx.Response(
        status_code=200,
        json=["olleh", "dlrow"],
        request=httpx.Request("POST", "http://test"),
    )
    with patch("exercise.tmprl.workflow.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=None)

        result = await flip_batch(["hello", "world"])

    assert result == BatchResult(strings=["olleh", "dlrow"])
    mock_client.post.assert_called_once()


@pytest.mark.asyncio
async def test_flip_batch_partial_failure() -> None:
    mock_response = httpx.Response(
        status_code=200,
        json=["olleh", "-"],
        request=httpx.Request("POST", "http://test"),
    )
    with patch("exercise.tmprl.workflow.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=None)

        result = await flip_batch(["hello", "world"])

    assert result == BatchResult(strings=["olleh", "-"])


@pytest.mark.asyncio
async def test_flip_batch_server_error_raises() -> None:
    mock_response = httpx.Response(
        status_code=500,
        json={"detail": "Random immediate failure occurred."},
        request=httpx.Request("POST", "http://test"),
    )
    with patch("exercise.tmprl.workflow.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=None)

        with pytest.raises(httpx.HTTPStatusError):
            await flip_batch(["hello"])


@pytest.mark.asyncio
async def test_flip_batch_bad_request_non_retryable() -> None:
    mock_response = httpx.Response(
        status_code=400,
        json={
            "detail": "Strings contain characters outside the current alphabet mode."
        },
        request=httpx.Request("POST", "http://test"),
    )
    with patch("exercise.tmprl.workflow.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=None)

        with pytest.raises(temporalio.exceptions.ApplicationError) as exc_info:
            await flip_batch(["Hello123"])

        assert exc_info.value.non_retryable
