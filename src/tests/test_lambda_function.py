"""Unit tests for the Lambda handler. These do not require an AWS account."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from lambda_function import (
    AGGREGATION_SQL,
    EventParseError,
    ObjectKeyError,
    decode_s3_object_key,
    handler,
    parse_s3_record,
    result_keys,
    run_aggregation,
    validate_object_key,
)


def s3_event(bucket: str, key: str, etag: str = "abc123") -> dict:
    return {
        "Records": [
            {
                "eventSource": "aws:s3",
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key, "eTag": etag},
                },
            }
        ]
    }


def test_decode_url_encoded_key_with_spaces() -> None:
    assert decode_s3_object_key("raw/sales/2026+Q1+orders.parquet") == (
        "raw/sales/2026 Q1 orders.parquet"
    )


def test_decode_url_encoded_key_with_percent_encoding() -> None:
    assert decode_s3_object_key("raw/sales/north%2Damerica.parquet") == (
        "raw/sales/north-america.parquet"
    )


def test_valid_s3_event_parsing() -> None:
    parsed = parse_s3_record(s3_event("example-bucket-1", "raw/sales/orders.parquet"))
    assert parsed["bucket"] == "example-bucket-1"
    assert parsed["key"] == "raw/sales/orders.parquet"
    assert parsed["etag"] == "abc123"


def test_etag_quotes_are_stripped() -> None:
    parsed = parse_s3_record(s3_event("example-bucket-1", "raw/sales/orders.parquet", '"deadbeef"'))
    assert parsed["etag"] == "deadbeef"


def test_invalid_event_missing_records() -> None:
    with pytest.raises(EventParseError):
        parse_s3_record({})


def test_invalid_event_missing_s3() -> None:
    with pytest.raises(EventParseError):
        parse_s3_record({"Records": [{"eventSource": "aws:s3"}]})


def test_results_prefix_is_rejected() -> None:
    with pytest.raises(ObjectKeyError):
        validate_object_key("results/output.parquet")


def test_non_parquet_is_rejected() -> None:
    with pytest.raises(ObjectKeyError):
        validate_object_key("raw/sales/notes.csv")


def test_path_traversal_is_rejected() -> None:
    with pytest.raises(ObjectKeyError):
        validate_object_key("raw/../secret.parquet")


def test_folder_placeholder_is_rejected() -> None:
    with pytest.raises(ObjectKeyError):
        validate_object_key("raw/sales/")


def test_result_keys_are_deterministic() -> None:
    parquet_a, json_a = result_keys("raw/sales/orders.parquet", "etag1")
    parquet_b, json_b = result_keys("raw/sales/orders.parquet", "etag1")
    assert parquet_a == parquet_b
    assert json_a == json_b
    assert parquet_a.startswith("results/")
    assert json_a.endswith(".json")
    parquet_c, _ = result_keys("raw/sales/orders.parquet", "etag2")
    assert parquet_a != parquet_c


def test_sql_aggregation_on_local_parquet(tmp_path: Path) -> None:
    source = tmp_path / "sales.parquet"
    con = duckdb.connect(database=":memory:")
    con.execute(
        """
        COPY (
            SELECT * FROM (
                VALUES
                    (1, 10, DATE '2026-01-01', 'US', 'Widgets', 2, 10.0),
                    (2, 11, DATE '2026-01-02', 'US', 'Widgets', 1, 10.0),
                    (3, 12, DATE '2026-01-03', 'DE', 'Gadgets', 5, 4.0)
            ) AS t(order_id, customer_id, order_date, country, product_category, quantity, unit_price)
        ) TO ? (FORMAT parquet)
        """,
        [str(source)],
    )
    rows = run_aggregation(con, str(source))
    by_country = {row[0]: row for row in rows}
    assert by_country["US"][2] == 3
    assert by_country["US"][3] == 30.0
    assert by_country["DE"][2] == 5
    assert by_country["DE"][3] == 20.0
    assert "read_parquet" in AGGREGATION_SQL
    con.close()


def test_write_results_creates_parquet_and_json(tmp_path: Path) -> None:
    from lambda_function import write_results

    source = tmp_path / "sales.parquet"
    parquet_out = tmp_path / "out.parquet"
    json_out = tmp_path / "out.json"
    con = duckdb.connect(database=":memory:")
    con.execute(
        """
        COPY (
            SELECT * FROM (
                VALUES (1, 10, DATE '2026-01-01', 'FR', 'Widgets', 3, 2.5)
            ) AS t(order_id, customer_id, order_date, country, product_category, quantity, unit_price)
        ) TO ? (FORMAT parquet)
        """,
        [str(source)],
    )
    write_results(con, str(source), str(parquet_out), str(json_out))
    assert parquet_out.exists()
    payload = json.loads(json_out.read_text())
    assert payload[0]["country"] == "FR"
    assert payload[0]["units_sold"] == 3
    con.close()


def test_handler_returns_ignored_for_invalid_event() -> None:
    response = handler({"hello": "world"}, None)
    assert response["status"] == "ignored"


def test_handler_success_with_stubbed_duckdb() -> None:
    event = s3_event("example-bucket-1", "raw/sales/orders.parquet", "etag99")
    fake_con = MagicMock()
    fake_con.execute.return_value.fetchall.return_value = [("US", "Widgets", 3, 30.0)]

    with (
        patch.dict("os.environ", {"AWS_REGION": "us-east-1"}, clear=False),
        patch("duckdb.connect", return_value=fake_con),
        patch("lambda_function.configure_duckdb"),
        patch("lambda_function.write_results"),
        patch("lambda_function.run_aggregation", return_value=[("US", "Widgets", 3, 30.0)]),
    ):
        response = handler(event, MagicMock(aws_request_id="req-1"))

    assert response["status"] == "ok"
    assert response["source_key"] == "raw/sales/orders.parquet"
    assert "results/" in response["result_parquet"]
    assert response["group_count"] == 1


def test_handler_success_with_api_gateway_event() -> None:
    event = {
        "version": "2.0",
        "routeKey": "GET /sales/summary",
        "rawPath": "/sales/summary",
        "rawQueryString": "",
        "headers": {},
        "requestContext": {"http": {"method": "GET", "path": "/sales/summary"}},
        "isBase64Encoded": False,
    }

    fake_con = MagicMock()
    fake_con.execute.return_value.fetchall.return_value = [("US", "Widgets", 3, 30.0)]

    with (
        patch.dict(
            "os.environ",
            {"AWS_REGION": "us-east-1", "SALES_BUCKET": "example-bucket-1", "SALES_PREFIX": "raw/sales"},
            clear=False,
        ),
        patch("duckdb.connect", return_value=fake_con),
        patch("lambda_function.configure_duckdb"),
        patch("lambda_function.run_aggregation", return_value=[("US", "Widgets", 3, 30.0)]),
    ):
        response = handler(event, MagicMock(aws_request_id="req-api"))

    assert response["statusCode"] == 200
    payload = json.loads(response["body"])
    assert payload["status"] == "ok"
    assert payload["rows"][0]["country"] == "US"
    assert payload["rows"][0]["revenue"] == 30.0


def test_handler_reraises_processing_errors() -> None:
    event = s3_event("example-bucket-1", "raw/sales/orders.parquet")
    with (
        patch.dict("os.environ", {"AWS_REGION": "us-east-1"}, clear=False),
        patch("duckdb.connect", side_effect=RuntimeError("duckdb failed")),
    ):
        with pytest.raises(RuntimeError, match="duckdb failed"):
            handler(event, None)
