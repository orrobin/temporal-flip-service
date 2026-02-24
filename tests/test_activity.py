import httpx
import pytest
import temporalio.exceptions

from exercise.workflows.workflow import BatchResult, flip_batch
from tests.conftest import mock_http_client


@pytest.mark.asyncio
async def test_flip_batch_success() -> None:
    response = httpx.Response(
        status_code=200,
        json=["olleh", "dlrow"],
        request=httpx.Request("POST", "http://test"),
    )
    with mock_http_client(response) as mock_client:
        result = await flip_batch(["hello", "world"])

    assert result == BatchResult(strings=["olleh", "dlrow"])
    mock_client.post.assert_called_once()


@pytest.mark.asyncio
async def test_flip_batch_partial_failure() -> None:
    response = httpx.Response(
        status_code=200,
        json=["olleh", "-"],
        request=httpx.Request("POST", "http://test"),
    )
    with mock_http_client(response):
        result = await flip_batch(["hello", "world"])

    assert result == BatchResult(strings=["olleh", "-"])


@pytest.mark.asyncio
async def test_flip_batch_server_error_raises() -> None:
    response = httpx.Response(
        status_code=500,
        json={"detail": "Random immediate failure occurred."},
        request=httpx.Request("POST", "http://test"),
    )
    with mock_http_client(response):
        with pytest.raises(httpx.HTTPStatusError):
            await flip_batch(["hello"])


@pytest.mark.asyncio
async def test_flip_batch_bad_request_non_retryable() -> None:
    response = httpx.Response(
        status_code=400,
        json={
            "detail": "Strings contain characters outside the current alphabet mode."
        },
        request=httpx.Request("POST", "http://test"),
    )
    with mock_http_client(response):
        with pytest.raises(temporalio.exceptions.ApplicationError) as exc_info:
            await flip_batch(["Hello123"])

        assert exc_info.value.non_retryable
