FROM python:3.15-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    NO_SHOW_URLS=1 \
    PYTHONPATH=/run/src \
    APPWORLD_CACHE=/run/.cache/appworld

WORKDIR /run

# Create non-root user early and set up cache directory
RUN groupadd -r appuser && useradd -r -g appuser -u 1000 appuser \
    && mkdir -p /home/appuser \
    && mkdir -p /run/.cache/appworld \
    && chown -R appuser:appuser /home/appuser /run

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential python3-dev git git-lfs curl \
    && git lfs install \
    && pip install "typer<0.13.0" "click<8.2.0"

# Switch to non-root user
USER appuser

# Add .local/bin to PATH for the appuser
ENV PATH="/home/appuser/.local/bin:${PATH}"

RUN git clone https://github.com/StonyBrookNLP/appworld.git /tmp/appworld \
    && cd /tmp/appworld \
    && git checkout 3f2d53c1a096805adc86bd05227463683cdc5154 \
    && git lfs pull \
    && pip install ".[mcp]" \
    && cd /run \
    && appworld install \
    && appworld download data \
    && chmod -R 700 /run/.cache/appworld

# Copy entrypoint after expensive build steps to preserve Docker layer cache
COPY --chown=appuser:appuser entrypoint.py /run/entrypoint.py

# Switch back to root to clean up and verify cache
USER root

# Verify cache directory exists and set final ownership
RUN ls -la /run/.cache/appworld || echo "Cache directory not found!" \
    && chown -R appuser:appuser /run/.cache \
    && chown -R appuser:appuser /run \
    && chown -R appuser:appuser /run/data

# Clean up only build tools, keep git and git-lfs
RUN apt-get purge -y --auto-remove build-essential python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Switch back to non-root user for runtime
USER appuser

EXPOSE 8000 8001

ENV APIS_PORT=8000 \
    APPWORLD_ROOT=/run \
    APIS_ON_DISK=1 \
    MCP_TRANSPORT=http \
    MCP_PORT=8001 \
    REMOTE_APIS_URL=http://localhost:8000

CMD ["python", "/run/entrypoint.py"]
