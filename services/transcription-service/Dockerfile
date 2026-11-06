# syntax=docker/dockerfile:1
#
# transcription-service: LiveKit audio hook -> Deepgram WSS proxy (port 8091).
#
# Build context is the repo root so the single root .dockerignore (which keeps every env file
# out of every image) applies to both service images:
#   docker build -f services/transcription-service/Dockerfile -t mytheus-transcription .
#
# Same shape as mytheus-backend/Dockerfile: uv builder -> slim runtime, non-root uid 10001, tini.
# The service has no imports from the top-level libs/ (that tree is TypeScript-only).

ARG PYTHON_IMAGE=python:3.12-slim

FROM ${PYTHON_IMAGE} AS builder
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
WORKDIR /app

# --python pins the image's own interpreter: .python-version names a patch release the base image
# may not have, and with downloads disabled uv would otherwise fail to resolve it.
COPY services/transcription-service/pyproject.toml services/transcription-service/uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project --python /usr/local/bin/python
COPY services/transcription-service/src ./src
# --no-editable installs the package into the venv as a wheel, so the runtime does not need src/.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --python /usr/local/bin/python


FROM ${PYTHON_IMAGE} AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 -m appuser
WORKDIR /app
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv
# The service owns the transcript_chunks migration history. Nothing in the production compose
# runs it today; it is shipped so a one-off `alembic upgrade head`
# can run from this same image (alembic.ini uses prepend_sys_path = .).
COPY --chown=appuser:appuser services/transcription-service/alembic.ini ./alembic.ini
COPY --chown=appuser:appuser services/transcription-service/migrations ./migrations
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
USER appuser
EXPOSE 8091

ENTRYPOINT ["tini", "--"]
# The production compose sets the same command explicitly; this keeps `docker run` working alone.
CMD ["uvicorn", "transcription_service.main:app", "--host", "0.0.0.0", "--port", "8091"]
