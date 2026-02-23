import pytest


@pytest.fixture
def successful_response() -> list[str]:
    return ["olleh", "dlrow"]


@pytest.fixture
def partial_failure_response() -> list[str]:
    return ["olleh", "-"]
