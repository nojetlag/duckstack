from __future__ import annotations

import json
import os

import duckdb
import httpx

from duckstack import cache


async def fetch_api_data(
    source: dict, runtime_params: dict, db: duckdb.DuckDBPyConnection
) -> tuple[list[str], list[list], bool]:
    """Fetch from an external API, cache the result, and return (columns, rows, was_cached)."""

    # Merge query params: source defaults + runtime overrides
    params = {**source["query_params"], **runtime_params}

    # Resolve API key
    api_key = _resolve_api_key(source)

    # Inject API key into params or headers
    headers: dict[str, str] = {}
    if source["api_key_param"] and api_key:
        params[source["api_key_param"]] = api_key
    elif source["auth_header"] and api_key:
        headers[source["auth_header"]] = f"Bearer {api_key}"

    # Check cache
    cache_key = f"{source['name']}:{sorted(params.items())}"
    cached_data = cache.get(cache_key)
    if cached_data is not None:
        return cached_data[0], cached_data[1], True

    # Fetch from API
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            source["endpoint_url"], params=params, headers=headers, timeout=30.0
        )
        resp.raise_for_status()
        json_data = resp.json()

    # Extract data array using response_path
    data = _extract_path(json_data, source["response_path"])

    # Convert to tabular via DuckDB
    columns, rows = _to_tabular(data, db)

    # Cache
    cache.put(cache_key, (columns, rows), source["ttl_seconds"])

    return columns, rows, False


def _resolve_api_key(source: dict) -> str:
    if source.get("api_key_override"):
        return source["api_key_override"]
    if source.get("auth_env_var"):
        return os.environ.get(source["auth_env_var"], "")
    return ""


def _extract_path(data: dict, path: str) -> list[dict]:
    """Navigate dot-separated path into JSON response."""
    for key in path.split("."):
        data = data[key]
    return data


def _to_tabular(
    records: list[dict], db: duckdb.DuckDBPyConnection
) -> tuple[list[str], list[list]]:
    """Use DuckDB read_json_auto to convert a list of dicts to columns+rows."""
    json_str = json.dumps(records)
    result = db.execute("SELECT * FROM read_json_auto(?)", [json_str])
    columns = [desc[0] for desc in result.description]
    rows = [list(r) for r in result.fetchall()]
    return columns, rows
