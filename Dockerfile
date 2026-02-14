# ============================================================
# Stage 1: Build (install dependencies + build wheel)
# ============================================================
FROM python:3.9-slim AS builder

WORKDIR /build

COPY pyproject.toml ./
COPY src/ ./src/

RUN pip install --no-cache-dir build \
    && python -m build --wheel --outdir /build/dist

RUN pip install --no-cache-dir --prefix=/install /build/dist/*.whl

# ============================================================
# Stage 2: Runtime (minimal image)
# ============================================================
FROM python:3.9-slim AS runtime

# OpenShift compatibility: non-root user in GID 0
# OpenShift assigns arbitrary UIDs but always uses GID 0
RUN groupadd --gid 1001 duckstack \
    && useradd --uid 1001 --gid 0 --no-create-home --shell /sbin/nologin duckstack

# Copy installed Python packages from builder
COPY --from=builder /install /usr/local

# Pre-install DuckDB httpfs extension into a persistent, executable location
RUN mkdir -p /opt/duckdb \
    && python -c "import duckdb; db = duckdb.connect(); db.execute(\"SET home_directory = '/opt/duckdb'\"); db.execute('INSTALL httpfs')"

WORKDIR /app

COPY data/ /app/data/

# Group-writable for OpenShift arbitrary UID support
RUN chown -R 1001:0 /app /opt/duckdb \
    && chmod -R g=u /app /opt/duckdb

EXPOSE 8000

USER 1001

ENV DATA_DIR=/app/data \
    DUCKDB_HOME=/opt/duckdb \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"]

CMD ["uvicorn", "duckstack.main:app", "--host", "0.0.0.0", "--port", "8000"]
