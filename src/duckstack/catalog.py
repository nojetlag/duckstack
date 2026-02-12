from __future__ import annotations

import asyncpg

CREATE_DATASETS = """
CREATE TABLE IF NOT EXISTS datasets (
    id          SERIAL PRIMARY KEY,
    name        TEXT UNIQUE NOT NULL,
    path        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

CREATE_COLUMNS = """
CREATE TABLE IF NOT EXISTS dataset_columns (
    id          SERIAL PRIMARY KEY,
    dataset_id  INT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    dtype       TEXT NOT NULL,
    UNIQUE(dataset_id, name)
)
"""


async def init_catalog(database_url: str) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(database_url)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_DATASETS)
        await conn.execute(CREATE_COLUMNS)
    return pool


async def list_datasets(pool: asyncpg.Pool) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, path, description, created_at FROM datasets ORDER BY id"
        )
    return [dict(r) for r in rows]


async def get_dataset(pool: asyncpg.Pool, name: str) -> dict | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, name, path, description, created_at FROM datasets WHERE name = $1",
            name,
        )
        if row is None:
            return None
        columns = await conn.fetch(
            "SELECT name, dtype FROM dataset_columns WHERE dataset_id = $1 ORDER BY id",
            row["id"],
        )
    return {**dict(row), "columns": [dict(c) for c in columns]}


async def create_dataset(
    pool: asyncpg.Pool,
    name: str,
    path: str,
    description: str,
    columns: list[dict],
) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "INSERT INTO datasets (name, path, description) VALUES ($1, $2, $3) RETURNING id, name, path, description, created_at",
                name,
                path,
                description,
            )
            for col in columns:
                await conn.execute(
                    "INSERT INTO dataset_columns (dataset_id, name, dtype) VALUES ($1, $2, $3)",
                    row["id"],
                    col["name"],
                    col["dtype"],
                )
    return {**dict(row), "columns": columns}


async def delete_dataset(pool: asyncpg.Pool, name: str) -> bool:
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM datasets WHERE name = $1", name)
    return result == "DELETE 1"
