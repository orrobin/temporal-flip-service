# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Developer Info
- **Name:** Or Robin
- **Position:** Backend Engineer
- **Email:** orrobin5823@gmail.com

## Project Overview

Flip Server Exercise — a Python service that uses [Temporal.io](https://temporal.io) to reliably flip strings via an unreliable HTTP server. The flip server randomly fails (500), partially fails (returns `"-"` instead of a flipped string), accepts only lowercase a-z, and processes at most 10 strings per request. The goal is to build a Temporal workflow that handles all these quirks and reliably returns flipped strings.

**Tech Stack:** Python 3.12, Temporal.io, FastAPI, Poetry

**Production-Grade Requirements:** The project must meet production-grade Python standards:
- Proper error handling, logging, and testing
- Typing, linting, and formatting
- Documentation as needed
- Packaged with [Poetry](https://python-poetry.org/) for easy installation
- `README.md` with instructions on how to run the code

**Requirements:**
1. Accept arbitrary number of strings to flip
2. Reliably flip all of them
3. Add temporal `query` for number of strings flipped so far (measures progress)
4. Add temporal `query` for number of `HTTP 200 OK` responses received so far from the unreliable server (measures how much real work we caused the flip server to do)

**Using the Queries:** `client.py` must run an asynchronous polling loop that queries the workflow and prints progress every `n` seconds.

**Expected CLI Usage:**
```bash
$ poetry run python exercise/client.py "hello" "world"
> "olleh" "dlrow"
```

**Bonus Points (ordered):**
1. Implement FastAPI server that wraps Temporal, i.e. `POST /flip` with JSON body `["hello", "world"]` returns `["olleh", "dlrow"]`
2. Containerize the service
3. Write Kubernetes deployment files

**Flip Server API:**
- Endpoint: `POST http://127.0.0.1:8000/flip`
- Content-Type: `application/json`
- Body: JSON array of strings, e.g. `["hello", "world"]`
- Response: JSON array of flipped strings, e.g. `["olleh", "dlrow"]`

## Run Commands

```bash
# Install dependencies
poetry install

# Start local Temporal server (required)
temporal server start-dev

# Run the flip server (black-box, do not modify)
poetry run python exercise/server.py

# Run the Temporal worker
poetry run python exercise/worker.py

# Run the client
poetry run python exercise/client.py "hello" "world"
```

## Test Commands

```bash
# Run all tests
poetry run pytest

# Run specific test
poetry run pytest -k "test_name"

# Run with verbose output
poetry run pytest -v
```

The test project uses pytest with async support.

## Linting & Formatting

```bash
# Format code
poetry run black .

# Sort imports
poetry run isort .

# Lint
poetry run pylint exercise/

# Type check
poetry run mypy exercise/
```

## Architecture

### Project Structure

- **`exercise/server.py`** — Black-box flip server (FastAPI). **Do not modify.** Quirks:
  - 10% chance of random 500 failure
  - 5-second processing delay per request
  - Only accepts lowercase a-z strings (else 400)
  - 10% per-string chance of returning `"-"` instead of the flip (partial failure on 200)
  - Max 10 strings per request (else 400)

- **`exercise/worker.py`** — Temporal worker process. Registers workflows and activities. New activities must be added to the `ACTIVITIES` list.

- **`exercise/client.py`** — CLI client that starts a Temporal workflow and returns results. Should include query polling for progress.

- **`exercise/workflows/workflow.py`** — Temporal workflow and activity definitions. Core implementation file.

- **`exercise/workflows/__init__.py`** — Package exports (`Flip`, `Params`, `Result`).

- **`pyproject.toml`** — Poetry project configuration and dependencies.

### Key Patterns

**Temporal Workflow/Activity Pattern** — Use `@workflow.defn` for workflows and `@activity.defn` for activities:
```python
@dataclasses.dataclass
class Params:
    strings: list[str]

@temporalio.activity.defn
async def flip_strings(batch: list[str]) -> list[str]:
    # Call the flip server via HTTP
    ...

@temporalio.workflow.defn(name="Flip", sandboxed=False)
class Flip:
    @temporalio.workflow.run
    async def run(self, params: Params) -> Result:
        # Orchestrate activities with retry logic
        ...
```

**Dataclass DTOs** — Use `@dataclasses.dataclass` for workflow input/output:
```python
@dataclasses.dataclass
class Params:
    strings: list[str]

@dataclasses.dataclass
class Result:
    strings: list[str]
```

**Async/Await** — All I/O operations (HTTP calls, Temporal activity execution) use async/await.

## Dependencies

### Runtime
- `fastapi` — Web framework (used by the flip server and optional API wrapper)
- `uvicorn` — ASGI server for FastAPI
- `temporalio` — Temporal SDK for Python

### Development
- `black` — Code formatter
- `mypy` — Static type checker
- `pylint` — Linter (disables `missing-docstring`, `too-few-public-methods`)
- `isort` — Import sorter

## Core Principles

### 1. Clean Code
- **Readability First**: Code should be self-documenting; favoring clarity over brevity
- **Meaningful Names**: Use descriptive names for classes, methods, variables, and files
- **Single Responsibility**: Each class/method should have one reason to change
- **DRY Principle**: Avoid code duplication; extract reusable components
- **Keep It Simple**: Avoid unnecessary complexity and over-engineering
- **No Magic Numbers/Strings**: Use named constants and configuration values

### 2. SOLID Principles

#### S - Single Responsibility Principle (SRP)
- Each class should have a single, well-defined responsibility
- Avoid "God Objects" that handle multiple concerns

#### O - Open/Closed Principle (OCP)
- Code should be open for extension, closed for modification
- Use abstraction (interfaces, abstract classes) to allow flexibility

#### L - Liskov Substitution Principle (LSP)
- Derived classes must be substitutable for their base classes
- Don't violate expected behavior contracts

#### I - Interface Segregation Principle (ISP)
- Clients should not depend on interfaces they don't use
- Create focused, minimal interfaces

#### D - Dependency Inversion Principle (DIP)
- Depend on abstractions, not concrete implementations
- Use dependency injection where applicable

### 3. Design Patterns

Select patterns that solve real problems — avoid over-engineering.

#### Relevant Patterns for This Project

| Pattern | Purpose | Application in This Project |
|---------|---------|---------------------------|
| **Facade** | Unified interface to complex subsystem | FastAPI wrapper hides Temporal workflow orchestration behind a simple `POST /flip` HTTP API |
| **Service Layer** | Application boundary with operation layer | Temporal workflow acts as the service layer, orchestrating activities between presentation (CLI/API) and infrastructure (HTTP calls) |
| **Dependency Injection** | Supply dependencies externally | Activities receive HTTP client/server URL via injection for testability |
| **Saga** | Manage distributed transactions | Workflow orchestrates multiple unreliable HTTP calls with partial failure tracking — Temporal is a Saga orchestration engine |

#### Pattern Selection Guidelines

**DO Use a Pattern When:**
- It solves a real, current problem
- It simplifies the code (not complicates it)
- It follows existing codebase conventions
- Multiple modules need the same logic (DRY)
- It enables testing

**DON'T Use a Pattern When:**
- Simple if/else is clearer (KISS principle)
- You're solving a hypothetical future problem (YAGNI)
- It requires 4+ new files for trivial logic
- The codebase doesn't use it elsewhere
- Only one implementation exists

#### Pattern Decision Template
For design reviews, document pattern decisions:
```
Pattern: [Name]
Decision: [ ] USE  [ ] REJECT
Reason: [Brief justification]
```

#### Existing Patterns in This Codebase
- **Temporal Workflow**: `exercise/workflows/workflow.py` — `Flip` workflow definition
- **Dataclass DTOs**: `Params`, `Result` in `exercise/workflows/workflow.py`
- **Worker Registration**: `exercise/worker.py` — `ACTIVITIES` and `WORKFLOWS` lists

### 4. Performance & Scalability
- **Async Operations**: Use async/await for all I/O-bound operations (HTTP calls, Temporal activities)
- **Batching**: Respect the server's 10-string limit; batch larger inputs
- **Connection Reuse**: Reuse HTTP client sessions across activity calls
- **Retry Strategy**: Use Temporal's retry policies with appropriate backoff for the unreliable server
- **Monitoring**: Design with logging and performance metrics in mind

## Code Style & Conventions

### Python Standards (PEP 8)
```
# Naming
- snake_case: functions, methods, variables, module names
- PascalCase: classes
- UPPER_CASE: constants
- _leading_underscore: private/internal

# Organization
- Imports at top (stdlib, third-party, local), sorted by isort
- Constants before classes
- Public methods before private methods

# Formatting
- Enforced by black (line length 88)
- Type hints on all function signatures
- Docstrings for public APIs (Google style)
```

### Code Comments
- Avoid obvious comments; focus on "why" not "what"
- Use docstrings for public functions and classes
- Keep comments up-to-date with code changes

## Questions to Ask Before Implementation

Before making changes, clarify:

### 1. Scope & Requirements
- Is the requirement clear and complete?
- Are there edge cases to consider?
- Do we need to maintain backward compatibility?

### 2. Architecture & Design
- Does this fit the existing architecture?
- Should we introduce a new pattern or component?
- Are there performance implications?

### 3. Testing & Validation
- How will this be tested?
- Are there existing tests to update?
- What's the impact analysis?

### 4. Deployment & Rollback
- Is this a breaking change?
- What's the rollback strategy?

## Validation Checklist

Before completing any task, validate:

- **Passes linting** (`pylint`) without errors or warnings
- **Passes type checking** (`mypy`) without errors
- **Formatted** with `black` and `isort`
- **Follows SOLID principles** — Single responsibility, proper abstractions
- **Design patterns applied** where appropriate, not over-engineered
- **Performance considered** — No obvious bottlenecks, proper async usage
- **Clean code standards** — Readable, maintainable, self-documenting
- **Existing tests pass** — No regression
- **Error handling** — Proper exception handling and logging
- **No code duplication** — DRY principle applied
- **Configuration externalized** — No hardcoded values (server URLs, timeouts as constants)

## Effort Levels & Approach

### High Effort Areas
- **Scalability**: Design for growth from the start
- **Performance**: Profile and optimize critical paths
- **Maintainability**: Invest in clean architecture upfront
- **Testing**: Comprehensive unit and integration tests

### Implementation Approach
1. Analyze requirements thoroughly
2. Design solution following SOLID + patterns
3. Implement incrementally with validation
4. Test comprehensively
5. Document decisions and trade-offs

## Questions Before Starting Work

Confirm before proceeding on ambiguous items:
- "Is this the intended behavior or a workaround?"
- "Should we refactor this area or make minimal changes?"
- "Are there performance implications I should be aware of?"
- "Does this break existing contracts or introduce technical debt?"

## Testing Conventions

Tests use pytest with async support:
```python
import pytest

@pytest.mark.asyncio
async def test_flip_single_string():
    # Arrange
    params = Params(strings=["hello"])

    # Act
    result = await flip(params)

    # Assert
    assert result.strings == ["olleh"]
```

Test naming convention: `test_<function>_<scenario>_<expected_result>`

## Git & Version Control

- **Commit Messages**: Clear, descriptive messages explaining the "why"
- **Code Review**: All changes should be reviewable and justified
- **Testing**: Ensure changes don't break existing functionality

## Important Files

- `README.md` — Exercise description, requirements, and server quirks
- `pyproject.toml` — Poetry config, dependencies, and tool settings
- `exercise/server.py` — Black-box flip server (reference for quirks, do not modify)
- `exercise/workflows/workflow.py` — Core workflow implementation
