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
import time
from typing import Any
from urllib.parse import unquote_plus

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

SAFE_KEY_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9/_.-]*$")
SAFE_BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")

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


def parse_s3_record(event: dict[str, Any]) -> dict[str, str]:
    """Extract bucket, decoded key, and ETag from a single-record S3 event."""
    records = event.get("Records")
    if not isinstance(records, list) or not records:
        raise EventParseError("Event does not contain Records")

    record = records[0]
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


def s3_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def result_keys(source_key: str, etag: str) -> tuple[str, str]:
    """Deterministic output keys so retries overwrite the same objects.

    S3 event deliveries can be retried. This project does not claim exactly-once
    processing. Writing to a key derived from the source key and ETag makes a
    retry of the same object version overwrite the same result.
    """
    stem = source_key.rsplit("/", 1)[-1]
    if stem.lower().endswith(".parquet"):
        stem = stem[: -len(".parquet")]
    safe_etag = etag or "unknown-etag"
    base = f"{RESULTS_PREFIX}{stem}/{safe_etag}"
    return f"{base}/sales_by_country_category.parquet", f"{base}/sales_by_country_category.json"


def aggregation_sql() -> str:
    return AGGREGATION_SQL


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


def run_aggregation(con: Any, source_uri: str) -> list[tuple[Any, ...]]:
    """Execute the example GROUP BY aggregation against a Parquet source."""
    return con.execute(AGGREGATION_SQL, [source_uri]).fetchall()


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


def write_results(con: Any, source_uri: str, parquet_uri: str, json_uri: str) -> None:
    """Write aggregation output as Parquet and JSON (S3 in AWS, local paths in tests)."""
    source_uri = _assert_safe_uri(source_uri)
    parquet_uri = _assert_safe_uri(parquet_uri)
    json_uri = _assert_safe_uri(json_uri)
    con.execute("CREATE OR REPLACE TABLE sales_agg AS " + AGGREGATION_SQL, [source_uri])
    con.execute(f"COPY sales_agg TO '{parquet_uri}' (FORMAT parquet)")
    con.execute(f"COPY sales_agg TO '{json_uri}' (FORMAT json, ARRAY true)")


def _http_api_summary(event: dict[str, Any], request_id: str | None, started: float) -> dict[str, Any]:
    """Build the standard API Gateway payload used by the live sales summary route."""
    bucket = os.environ.get("SALES_BUCKET")
    if not bucket:
        raise RuntimeError("SALES_BUCKET is not set")

    prefix = os.environ.get("SALES_PREFIX", "raw/sales").strip("/")
    filters = event.get("queryStringParameters") or event.get("params") or {}
    if not isinstance(filters, dict):
        filters = {}

    _json_log(logging.INFO, "request.received", keys=list(event.keys()), request_id=request_id)
    _json_log(logging.INFO, "request.params", params=filters, request_id=request_id)
    _json_log(logging.INFO, "query.start", bucket=bucket, prefix=prefix, filters=filters, request_id=request_id)

    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not region:
        raise RuntimeError("AWS_REGION is not set")

    source_glob = f"s3://{bucket}/{prefix}/*.parquet" if prefix else f"s3://{bucket}/*.parquet"
    import duckdb

    con = duckdb.connect(database=":memory:")
    try:
        configure_duckdb(con, region)
        rows = con.execute(AGGREGATION_SQL, [source_glob]).fetchall()
    finally:
        con.close()

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


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    started = time.perf_counter()
    request_id = getattr(context, "aws_request_id", None)

    try:
        _log_stage("handler.entry", started, request_id=request_id)

        if isinstance(event, dict) and "Records" in event:
            parsed = parse_s3_record(event)
            bucket = parsed["bucket"]
            key = parsed["key"]
            etag = parsed["etag"]
            source = s3_uri(bucket, key)
            parquet_key, json_key = result_keys(key, etag)
            parquet_uri = s3_uri(bucket, parquet_key)
            json_uri = s3_uri(bucket, json_key)
            region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
            if not region:
                raise RuntimeError("AWS_REGION is not set")

            _json_log(
                logging.INFO,
                "received_s3_event",
                bucket=bucket,
                key=key,
                etag=etag,
                request_id=request_id,
            )
            _log_stage("after_event_parse", started, request_id=request_id, bucket=bucket, key=key)

            import duckdb

            _json_log(logging.INFO, "query_start", source_uri=source)
            con = duckdb.connect(database=":memory:")
            _log_stage("after_duckdb_connect", started, request_id=request_id, source_uri=source)
            try:
                configure_duckdb(con, region)
                _log_stage("after_duckdb_config", started, request_id=request_id, source_uri=source)
                write_results(con, source, parquet_uri, json_uri)
                _log_stage("after_write_results", started, request_id=request_id, output_parquet=parquet_uri, output_json=json_uri)
                rows = run_aggregation(con, source)
                _log_stage("after_run_aggregation", started, request_id=request_id, rows=len(rows))
            finally:
                con.close()

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
