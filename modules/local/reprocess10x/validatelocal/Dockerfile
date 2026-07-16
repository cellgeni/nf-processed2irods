# syntax=docker/dockerfile:1

##############################
# Builder stage
##############################
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

# Build psycopg2 needs the PostgreSQL client headers and a compiler.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# uv configuration: copy (not hardlink) into the image, compile bytecode,
# and place the virtualenv at a predictable location inside the project.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app

# Install dependencies first (cached layer) using only the lockfile + manifest,
# so application code changes don't invalidate the dependency layer.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# Now copy the source and install the project itself.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY schema ./schema
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

##############################
# Runtime stage
##############################
FROM python:3.12-slim-bookworm AS runtime

# psycopg2 needs the libpq runtime library (not the -dev headers).
# procps provides `ps`, which Nextflow requires to collect task metrics.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq5 \
        procps \
    && rm -rf /var/lib/apt/lists/*

# Run as a non-root user.
RUN useradd --create-home --uid 1000 tracking

WORKDIR /app

# Copy the fully-built virtualenv and the project source from the builder.
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
COPY --from=builder /app/schema /app/schema

# Put the virtualenv on PATH so the `sample-tracking` entry point is available.
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    LOCAL_SCHEMA_FILE="/app/schema/local_dataset_root.yml" \
    IRODS_SCHEMA_FILE="/app/schema/dataset_root.yml"

USER tracking

# No ENTRYPOINT: Nextflow invokes its own `.command.sh` via the shell and needs
# to run arbitrary commands (e.g. `sample-tracking`, `cat`, `ps`). A hardcoded
# ENTRYPOINT would be prepended to every command under the Docker executor.
CMD ["bash"]