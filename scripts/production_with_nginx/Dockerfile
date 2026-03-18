# --------- Builder Stage ---------
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_CACHE_DIR=/tmp/uv-cache

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY beampipe-wallaby /app/beampipe-wallaby
COPY src /app/src

RUN mkdir -p /tmp/uv-cache && chmod 777 /tmp/uv-cache \
    && uv sync --locked --no-editable --no-dev

# --------- Final Stage ---------
FROM python:3.11-slim-bookworm

RUN groupadd --gid 1000 app \
    && useradd --uid 1000 --gid app --shell /bin/bash --create-home app

COPY --from=builder --chown=app:app /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

USER app
WORKDIR /code

COPY --from=builder --chown=app:app /app/src/app /code/app

CMD ["gunicorn", "app.main:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "-b", "0.0.0.0:8000"]
