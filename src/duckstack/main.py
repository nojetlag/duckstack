from contextlib import asynccontextmanager
from pathlib import Path

import duckdb
from fastapi import FastAPI, HTTPException

from duckstack import catalog
from duckstack.config import settings
from duckstack.schemas import (
    DatasetCreate,
    DatasetDetail,
    DatasetSummary,
    QueryRequest,
    QueryResponse,
)

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

db = duckdb.connect()
db.execute(f"SET file_search_path = '{DATA_DIR}'")

# S3 support via httpfs
db.execute("INSTALL httpfs")
db.execute("LOAD httpfs")
if settings.aws_access_key_id:
    db.execute(f"SET s3_access_key_id = '{settings.aws_access_key_id}'")
    db.execute(f"SET s3_secret_access_key = '{settings.aws_secret_access_key}'")
    db.execute(f"SET s3_region = '{settings.aws_region}'")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.database_url:
        app.state.catalog_pool = await catalog.init_catalog(settings.database_url)
    else:
        app.state.catalog_pool = None
    yield
    if app.state.catalog_pool is not None:
        await app.state.catalog_pool.close()


app = FastAPI(title="Duckstack", version="0.1.0", lifespan=lifespan)


def _require_catalog(app: FastAPI):
    pool = getattr(app.state, "catalog_pool", None)
    if pool is None:
        raise HTTPException(status_code=503, detail="Catalog not configured (DATABASE_URL not set)")
    return pool


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest):
    try:
        result = db.execute(req.sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return QueryResponse(
            columns=columns,
            rows=[list(r) for r in rows],
            row_count=len(rows),
        )
    except duckdb.Error as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/datasets", response_model=list[DatasetSummary])
async def list_datasets():
    pool = _require_catalog(app)
    datasets = await catalog.list_datasets(pool)
    return datasets


@app.post("/datasets", response_model=DatasetDetail, status_code=201)
async def create_dataset(body: DatasetCreate):
    pool = _require_catalog(app)

    # Infer columns from the parquet file using DuckDB
    try:
        result = db.execute(f"DESCRIBE SELECT * FROM '{body.path}'")
        columns = [{"name": row[0], "dtype": row[1]} for row in result.fetchall()]
    except duckdb.Error as e:
        raise HTTPException(status_code=400, detail=f"Cannot read file: {e}")

    try:
        dataset = await catalog.create_dataset(
            pool, body.name, body.path, body.description, columns
        )
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(status_code=409, detail=f"Dataset '{body.name}' already exists")
        raise

    return dataset


@app.get("/datasets/{name}", response_model=DatasetDetail)
async def get_dataset(name: str):
    pool = _require_catalog(app)
    dataset = await catalog.get_dataset(pool, name)
    if dataset is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{name}' not found")
    return dataset


@app.delete("/datasets/{name}", status_code=204)
async def delete_dataset(name: str):
    pool = _require_catalog(app)
    deleted = await catalog.delete_dataset(pool, name)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Dataset '{name}' not found")
