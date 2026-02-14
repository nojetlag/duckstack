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

CREATE_API_SOURCES = """
CREATE TABLE IF NOT EXISTS api_sources (
    id              SERIAL PRIMARY KEY,
    name            TEXT UNIQUE NOT NULL,
    endpoint_url    TEXT NOT NULL,
    query_params    JSONB NOT NULL DEFAULT '{}',
    auth_header     TEXT NOT NULL DEFAULT '',
    auth_env_var    TEXT NOT NULL DEFAULT '',
    api_key_param   TEXT NOT NULL DEFAULT '',
    api_key_override TEXT NOT NULL DEFAULT '',
    response_path   TEXT NOT NULL DEFAULT 'results',
    ttl_seconds     INT NOT NULL DEFAULT 300,
    description     TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


async def init_catalog(database_url: str) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(database_url)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_DATASETS)
        await conn.execute(CREATE_COLUMNS)
        await conn.execute(CREATE_API_SOURCES)
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


# --- API Sources ---

_API_SOURCE_COLS = "id, name, endpoint_url, query_params, auth_header, auth_env_var, api_key_param, api_key_override, response_path, ttl_seconds, description, created_at"
_API_SOURCE_SUMMARY_COLS = "id, name, endpoint_url, description, ttl_seconds, created_at"


async def list_api_sources(pool: asyncpg.Pool) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT {_API_SOURCE_SUMMARY_COLS} FROM api_sources ORDER BY id"
        )
    return [dict(r) for r in rows]


async def get_api_source(pool: asyncpg.Pool, name: str) -> dict | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT {_API_SOURCE_COLS} FROM api_sources WHERE name = $1",
            name,
        )
    if row is None:
        return None
    result = dict(row)
    import json
    if isinstance(result["query_params"], str):
        result["query_params"] = json.loads(result["query_params"])
    return result


async def create_api_source(
    pool: asyncpg.Pool,
    name: str,
    endpoint_url: str,
    query_params: dict,
    auth_header: str,
    auth_env_var: str,
    api_key_param: str,
    api_key_override: str,
    response_path: str,
    ttl_seconds: int,
    description: str,
) -> dict:
    import json
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO api_sources (name, endpoint_url, query_params, auth_header, auth_env_var, api_key_param, api_key_override, response_path, ttl_seconds, description)
            VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id, name, endpoint_url, query_params, auth_header, auth_env_var, api_key_param, response_path, ttl_seconds, description, created_at""",
            name,
            endpoint_url,
            json.dumps(query_params),
            auth_header,
            auth_env_var,
            api_key_param,
            api_key_override,
            response_path,
            ttl_seconds,
            description,
        )
    result = dict(row)
    if isinstance(result["query_params"], str):
        result["query_params"] = json.loads(result["query_params"])
    return result


async def delete_api_source(pool: asyncpg.Pool, name: str) -> bool:
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM api_sources WHERE name = $1", name)
    return result == "DELETE 1"
