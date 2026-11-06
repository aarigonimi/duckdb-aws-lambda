"""AWS Lambda handler: DuckDB analytics on Parquet objects in Amazon S3.

This module is designed so that parsing, SQL, and DuckDB setup can be unit-tested
without an AWS account. S3 I/O in production is performed by DuckDB using
read_parquet / COPY against s3:// URIs.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from typing import Any
from urllib.parse import unquote_plus

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

SAFE_KEY_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9/_.-]*$")
SAFE_BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
SUPPORTED_QUERY_PARAMS = {"country", "year"}
MIN_YEAR = 1970
MAX_YEAR = 2100
MAX_COUNTRY_LENGTH = 100

RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/")
RESULTS_PREFIX = os.environ.get("RESULTS_PREFIX", "results/")
EXTENSION_DIRECTORY = os.environ.get(
    "DUCKDB_EXTENSION_DIRECTORY", "/opt/duckdb_extensions"
)

AGGREGATION_SQL = """
SELECT
    country,
    product_category,
    SUM(quantity) AS units_sold,
    SUM(quantity * unit_price) AS revenue
FROM read_parquet(?)
GROUP BY
    country,
    product_category
ORDER BY revenue DESC
"""

_DUCKDB_CONNECTION: Any | None = None
_DUCKDB_CONNECTION_LOCK = threading.Lock()
_DUCKDB_CONNECTION_REGION: str | None = None


class EventParseError(ValueError):
    """Raised when the Lambda event is not a usable S3 notification."""


class ObjectKeyError(ValueError):
    """Raised when an S3 object key is missing, encoded incorrectly, or unsafe."""


def _json_log(level: int, message: str, **fields: Any) -> None:
    payload = {"message": message, **fields}
    LOGGER.log(level, json.dumps(payload, default=str))


def _log_stage(stage: str, started_at: float, request_id: str | None = None, **fields: Any) -> None:
    payload = {
        "stage": stage,
        "elapsed_ms": round((time.perf_counter() - started_at) * 1000, 2),
        "request_id": request_id,
        **fields,
    }
    _json_log(logging.INFO, "stage", **payload)


def decode_s3_object_key(encoded_key: str) -> str:
    """Decode an S3 event object key.

    S3 notification keys use URL encoding (plus signs for spaces). See:
    https://docs.aws.amazon.com/lambda/latest/dg/with-s3.html
    """
    if not encoded_key or not isinstance(encoded_key, str):
        raise ObjectKeyError("S3 object key is missing")
    return unquote_plus(encoded_key)


def validate_object_key(key: str, raw_prefix: str = RAW_PREFIX) -> str:
    """Reject keys that must not be processed (output prefixes, path traversal)."""
    if key.endswith("/"):
        raise ObjectKeyError("S3 object key appears to be a folder placeholder")
    if ".." in key:
        raise ObjectKeyError("S3 object key must not contain '..'")
    if not SAFE_KEY_PATTERN.match(key):
        raise ObjectKeyError("S3 object key contains unsupported characters")
    if not key.startswith(raw_prefix):
        raise ObjectKeyError(f"S3 object key is outside the {raw_prefix} prefix")
    if key.startswith(RESULTS_PREFIX):
        raise ObjectKeyError("S3 object key is under the results prefix")
    if not key.lower().endswith(".parquet"):
        raise ObjectKeyError("S3 object key must end with .parquet")
    return key


def validate_query_params(filters: dict[str, Any] | None) -> dict[str, Any]:
    """Validate and normalize supported HTTP query parameters."""
    if filters is None:
        return {}
    if not isinstance(filters, dict):
        raise ValueError("Query parameters must be a dictionary")

    unsupported = sorted(set(filters) - SUPPORTED_QUERY_PARAMS)
    if unsupported:
        raise ValueError(f"Unsupported query parameter: {unsupported[0]}")

    cleaned: dict[str, Any] = {}
    country_value = filters.get("country")
    if country_value is not None:
        if not isinstance(country_value, str):
            raise ValueError("country parameter must be a string")
        country = country_value.strip()
        if not country:
            raise ValueError("country parameter must not be empty")
        if len(country) > MAX_COUNTRY_LENGTH:
            raise ValueError("country parameter exceeds 100 characters")
        if not re.fullmatch(r"[A-Za-z0-9\-_. /]+", country):
            raise ValueError("country parameter contains unsupported characters")
        cleaned["country"] = country

    year_value = filters.get("year")
    if year_value is not None:
        if isinstance(year_value, bool):
            raise ValueError("year parameter must be an integer year")
        if isinstance(year_value, str):
            if not year_value.strip():
                raise ValueError("year parameter must not be empty")
            try:
                year = int(year_value)
            except ValueError as exc:
                raise ValueError("year parameter must be a valid integer") from exc
        elif isinstance(year_value, int):
            year = year_value
        else:
            raise ValueError("year parameter must be an integer or a year string")

        if year < MIN_YEAR or year > MAX_YEAR:
            raise ValueError(f"year parameter must be between {MIN_YEAR} and {MAX_YEAR}")
        cleaned["year"] = year

    return cleaned


def parse_single_record(record: dict[str, Any]) -> dict[str, str]:
    s3 = record.get("s3") if isinstance(record, dict) else None
    if not isinstance(s3, dict):
        raise EventParseError("Event record is missing s3 metadata")

    bucket_obj = s3.get("bucket") or {}
    object_obj = s3.get("object") or {}
    bucket = bucket_obj.get("name")
    encoded_key = object_obj.get("key")
    etag = object_obj.get("eTag") or object_obj.get("etag") or ""

    if not bucket or not isinstance(bucket, str):
        raise EventParseError("S3 bucket name is missing")
    if not SAFE_BUCKET_PATTERN.match(bucket):
        raise EventParseError("S3 bucket name is invalid")

    key = validate_object_key(decode_s3_object_key(encoded_key))
    etag_clean = str(etag).strip('"')
    return {"bucket": bucket, "key": key, "etag": etag_clean}


def parse_s3_record(event: dict[str, Any]) -> dict[str, str]:
    """Extract bucket, decoded key, and ETag from a single-record S3 event."""
    records = event.get("Records")
    if not isinstance(records, list) or not records:
        raise EventParseError("Event does not contain Records")
    return parse_single_record(records[0])


def parse_s3_records(event: dict[str, Any]) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    records = event.get("Records")
    if not isinstance(records, list) or not records:
        raise EventParseError("Event does not contain Records")

    valid_records: list[dict[str, str]] = []
    invalid_records: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        try:
            valid_records.append(parse_single_record(record))
        except Exception as exc:  # pragma: no cover - exercised in unit tests via handler
            invalid_records.append({"index": index, "error": str(exc)})
    if not valid_records:
        raise EventParseError("Event contains no valid S3 records")
    return valid_records, invalid_records


def s3_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def _sanitize_result_fragment(raw: str) -> str:
    fragment = raw.strip().strip("/")
    fragment = fragment.replace("..", "")
    fragment = re.sub(r"[^a-zA-Z0-9/_.-]+", "-", fragment)
    fragment = re.sub(r"/+", "/", fragment)
    return fragment.strip("/") or "object"


def result_keys(source_key: str, etag: str) -> tuple[str, str]:
    """Produce deterministic output keys that include the full source path.

    The key is derived from the source key plus the object version ETag so retries
    of the same object version overwrite the same result while distinct paths do
    not collide.
    """
    base_key = _sanitize_result_fragment(source_key.rsplit(".parquet", 1)[0] if source_key.lower().endswith(".parquet") else source_key)
    safe_etag = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(etag or "unknown-etag"))
    base = f"{RESULTS_PREFIX}{base_key}/{safe_etag}"
    return f"{base}/sales_by_country_category.parquet", f"{base}/sales_by_country_category.json"


def aggregation_sql() -> str:
    return AGGREGATION_SQL


def build_aggregation_query(source_uri: str, filters: dict[str, Any] | None = None) -> tuple[str, list[Any]]:
    query = """
        SELECT
            country,
            product_category,
            SUM(quantity) AS units_sold,
            SUM(quantity * unit_price) AS revenue
        FROM read_parquet(?)
    """
    params: list[Any] = [source_uri]
    clauses: list[str] = []
    normalized_filters = filters or {}
    if "country" in normalized_filters:
        clauses.append("LOWER(country) = LOWER(?)")
        params.append(normalized_filters["country"])
    if "year" in normalized_filters:
        clauses.append("EXTRACT(YEAR FROM order_date) = CAST(? AS INTEGER)")
        params.append(normalized_filters["year"])
    if clauses:
        query = f"{query} WHERE {' AND '.join(clauses)}\n"
    query += """
        GROUP BY
            country,
            product_category
        ORDER BY revenue DESC, country, product_category
    """
    return query, params


def configure_duckdb(con: Any, region: str) -> None:
    """Load AWS/S3 extensions and use the AWS credential chain.

    DuckDB's aws extension authenticates with CREATE SECRET ... PROVIDER credential_chain.
    See https://duckdb.org/docs/stable/core_extensions/aws.html

    Credentials are never passed as KEY_ID / SECRET. Lambda supplies credentials
    through the execution role; the AWS SDK default chain is used.
    """
    con.execute(f"SET extension_directory = '{EXTENSION_DIRECTORY}'")
    con.execute("SET home_directory = '/tmp'")
    con.execute("SET temp_directory = '/tmp'")
    con.execute("LOAD httpfs")
    con.execute("LOAD aws")
    con.execute("LOAD json")
    escaped_region = region.replace("'", "")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET lambda_s3 (
            TYPE s3,
            PROVIDER credential_chain,
            REGION '{escaped_region}'
        )
        """
    )


def get_duckdb_connection(region: str) -> Any:
    """Return a lazily initialized connection shared within one execution environment."""
    global _DUCKDB_CONNECTION, _DUCKDB_CONNECTION_REGION
    with _DUCKDB_CONNECTION_LOCK:
        if _DUCKDB_CONNECTION is None:
            import duckdb

            _DUCKDB_CONNECTION = duckdb.connect(database=":memory:")
            _DUCKDB_CONNECTION_REGION = None
        if _DUCKDB_CONNECTION_REGION != region:
            try:
                configure_duckdb(_DUCKDB_CONNECTION, region)
                _DUCKDB_CONNECTION_REGION = region
            except Exception:
                try:
                    _DUCKDB_CONNECTION.close()
                except Exception:
                    pass
                _DUCKDB_CONNECTION = None
                _DUCKDB_CONNECTION_REGION = None
                raise
        return _DUCKDB_CONNECTION


def run_aggregation(con: Any, source_uri: str, filters: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
    """Execute the example GROUP BY aggregation against a Parquet source."""
    query, params = build_aggregation_query(source_uri, filters)
    return con.execute(query, params).fetchall()


def resolve_aws_region() -> str:
    """Read the AWS region from a Lambda-safe environment key.

    AWS_REGION is a reserved Lambda runtime variable, so the application stores its
    region under APP_AWS_REGION and falls back to the standard AWS_DEFAULT_REGION
    or AWS_REGION values for compatibility with tests and local execution.
    """
    for key in ("APP_AWS_REGION", "AWS_DEFAULT_REGION", "AWS_REGION"):
        value = os.environ.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    raise RuntimeError("AWS region is not configured")


def _assert_safe_uri(uri: str) -> str:
    """Allow only local paths (tests) or s3:// URIs with conservative characters."""
    if uri.startswith("s3://"):
        rest = uri[len("s3://") :]
        if "/" not in rest:
            raise ValueError("S3 URI is missing an object key")
        bucket, key = rest.split("/", 1)
        if not SAFE_BUCKET_PATTERN.match(bucket) or not SAFE_KEY_PATTERN.match(key):
            raise ValueError("S3 URI contains unsupported characters")
        return uri
    if "://" in uri:
        raise ValueError("Unsupported URI scheme")
    if ".." in uri:
        raise ValueError("Local path must not contain '..'")
    return uri


def write_results(con: Any, source_uri: str, parquet_uri: str, json_uri: str, filters: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
    """Write aggregation output as Parquet and JSON using one materialized DuckDB result."""
    source_uri = _assert_safe_uri(source_uri)
    parquet_uri = _assert_safe_uri(parquet_uri)
    json_uri = _assert_safe_uri(json_uri)

    query, params = build_aggregation_query(source_uri, filters)
    con.execute("CREATE OR REPLACE TEMP TABLE sales_agg AS " + query, params)
    rows = con.execute("SELECT * FROM sales_agg ORDER BY revenue DESC, country, product_category").fetchall()
    con.execute(f"COPY sales_agg TO '{parquet_uri}' (FORMAT parquet)")
    con.execute(f"COPY sales_agg TO '{json_uri}' (FORMAT json, ARRAY true)")
    return rows


def _http_api_summary(event: dict[str, Any], request_id: str | None, started: float) -> dict[str, Any]:
    """Build the standard API Gateway payload used by the live sales summary route."""
    bucket = os.environ.get("SALES_BUCKET")
    if not bucket:
        raise RuntimeError("SALES_BUCKET is not set")

    prefix = os.environ.get("SALES_PREFIX", "raw/sales").strip("/")
    params = event.get("queryStringParameters") or event.get("params") or {}
    if not isinstance(params, dict):
        params = {}

    try:
        filters = validate_query_params(params)
    except ValueError as exc:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"status": "error", "error": str(exc)}),
        }

    _json_log(logging.INFO, "request.received", keys=list(event.keys()), request_id=request_id)
    _json_log(logging.INFO, "request.params", params=params, request_id=request_id)
    _json_log(logging.INFO, "query.start", bucket=bucket, prefix=prefix, filters=filters, request_id=request_id)

    region = resolve_aws_region()

    source_glob = f"s3://{bucket}/{prefix}/*.parquet" if prefix else f"s3://{bucket}/*.parquet"
    con = get_duckdb_connection(region)
    rows = run_aggregation(con, source_glob, filters=filters)

    duration_ms = round((time.perf_counter() - started) * 1000, 2)
    _json_log(
        logging.INFO,
        "query.completed",
        duration=duration_ms / 1000,
        rows=len(rows),
        request_id=request_id,
    )

    payload = {
        "status": "ok",
        "bucket": bucket,
        "prefix": prefix,
        "filters": filters,
        "rows": [
            {
                "country": row[0],
                "product_category": row[1],
                "units_sold": row[2],
                "revenue": row[3],
            }
            for row in rows
        ],
    }
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload, default=str),
    }


def _process_s3_record(record: dict[str, str], request_id: str | None, started: float) -> dict[str, Any]:
    bucket = record["bucket"]
    key = record["key"]
    etag = record["etag"]
    source = s3_uri(bucket, key)
    parquet_key, json_key = result_keys(key, etag)
    parquet_uri = s3_uri(bucket, parquet_key)
    json_uri = s3_uri(bucket, json_key)
    region = resolve_aws_region()

    _json_log(logging.INFO, "received_s3_event", bucket=bucket, key=key, etag=etag, request_id=request_id)
    _log_stage("after_event_parse", started, request_id=request_id, bucket=bucket, key=key)

    con = get_duckdb_connection(region)
    _log_stage("after_duckdb_connect", started, request_id=request_id, source_uri=source)
    try:
        rows = write_results(con, source, parquet_uri, json_uri)
        if not isinstance(rows, list):
            rows = run_aggregation(con, source)
        _log_stage("after_write_results", started, request_id=request_id, output_parquet=parquet_uri, output_json=json_uri, rows=len(rows))
    except Exception:
        try:
            with _DUCKDB_CONNECTION_LOCK:
                global _DUCKDB_CONNECTION
                if _DUCKDB_CONNECTION is not None:
                    _DUCKDB_CONNECTION.close()
                    _DUCKDB_CONNECTION = None
                    global _DUCKDB_CONNECTION_REGION
                    _DUCKDB_CONNECTION_REGION = None
        finally:
            raise

    duration_ms = round((time.perf_counter() - started) * 1000, 2)
    _json_log(
        logging.INFO,
        "query_complete",
        result_parquet=parquet_uri,
        result_json=json_uri,
        group_count=len(rows),
        duration_ms=duration_ms,
        request_id=request_id,
    )
    _log_stage("before_return", started, request_id=request_id, group_count=len(rows), duration_ms=duration_ms)
    return {
        "status": "ok",
        "bucket": bucket,
        "source_key": key,
        "result_parquet": parquet_uri,
        "result_json": json_uri,
        "group_count": len(rows),
        "duration_ms": duration_ms,
    }


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    started = time.perf_counter()
    request_id = getattr(context, "aws_request_id", None) if context else None

    try:
        _log_stage("handler.entry", started, request_id=request_id)

        if isinstance(event, dict) and "Records" in event:
            valid_records, invalid_records = parse_s3_records(event)
            results: list[dict[str, Any]] = []
            failed_records: list[dict[str, Any]] = [
                {"index": item["index"], "error": item["error"]} for item in invalid_records
            ]

            for record in valid_records:
                try:
                    processed = _process_s3_record(record, request_id, started)
                    if isinstance(processed.get("group_count"), int) and processed["group_count"] >= 0:
                        results.append(processed)
                    else:
                        results.append(processed)
                except Exception as exc:
                    failed_records.append({"bucket": record["bucket"], "key": record["key"], "error": str(exc)})

            if not results and failed_records:
                raise RuntimeError(failed_records[0]["error"])

            if len(results) == 1 and not failed_records:
                return results[0]

            return {
                "status": "partial_success" if failed_records else "ok",
                "processed_records": len(results),
                "failed_records": failed_records,
                "results": results,
            }

        if isinstance(event, dict) and any(key in event for key in ("routeKey", "requestContext", "rawPath", "headers")):
            return _http_api_summary(event, request_id, started)

        raise EventParseError("Event does not contain Records")
    except (EventParseError, ObjectKeyError) as exc:
        _json_log(logging.ERROR, "event_error", error=str(exc), request_id=request_id)
        return {"status": "ignored", "error": str(exc)}
    except Exception as exc:
        duration_ms = round((time.perf_counter() - started) * 1000, 2)
        _json_log(
            logging.ERROR,
            "processing_error",
            error=str(exc),
            duration_ms=duration_ms,
            request_id=request_id,
        )
        raise
