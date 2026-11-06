from __future__ import annotations

import json
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "sample-data" / "benchmark-large"
RESULT_FILE = ROOT.parent / "measurements" / "raw" / "local-benchmark-large.txt"

SQL = """
SELECT
    country,
    product_category,
    SUM(quantity) AS units_sold,
    SUM(quantity * unit_price) AS revenue
FROM read_parquet(?)
GROUP BY country, product_category
ORDER BY revenue DESC
"""


def generate_wide_parquet(path: Path, rows: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=":memory:")
    sql = f"""
    COPY (
        SELECT
            i AS order_id,
            (i * 17) % 100000 AS customer_id,
            DATE '2024-01-01' + CAST((i % 365) AS INTEGER) AS order_date,
            CASE WHEN i % 6 = 0 THEN 'US' WHEN i % 6 = 1 THEN 'DE' WHEN i % 6 = 2 THEN 'FR' WHEN i % 6 = 3 THEN 'JP' WHEN i % 6 = 4 THEN 'IN' ELSE 'BR' END AS country,
            CASE WHEN i % 7 = 0 THEN 'Electronics' WHEN i % 7 = 1 THEN 'Home' WHEN i % 7 = 2 THEN 'Books' WHEN i % 7 = 3 THEN 'Apparel' WHEN i % 7 = 4 THEN 'Toys' WHEN i % 7 = 5 THEN 'Grocery' ELSE 'Beauty' END AS product_category,
            (i % 9) + 1 AS quantity,
            ROUND((((i * 13) % 199) + 5.5), 2) AS unit_price,
            (i % 1000) AS v1,
            (i * 7) % 5000 AS v2,
            (i * 11) % 7000 AS v3,
            (i * 13) % 9000 AS v4,
            (i * 17) % 11000 AS v5,
            (i * 19) % 13000 AS v6,
            (i * 23) % 15000 AS v7,
            (i * 29) % 17000 AS v8,
            (i * 31) % 19000 AS v9,
            (i * 37) % 21000 AS v10,
            (i * 41) % 23000 AS v11,
            (i * 43) % 25000 AS v12,
            (i * 47) % 27000 AS v13,
            (i * 53) % 29000 AS v14,
            (i * 59) % 31000 AS v15,
            (i * 61) % 33000 AS v16,
            (i * 67) % 35000 AS v17,
            (i * 71) % 37000 AS v18,
            (i * 73) % 39000 AS v19,
            (i * 79) % 41000 AS v20
        FROM range(1, {rows} + 1) AS t(i))
        TO '{path}' (FORMAT parquet, COMPRESSION zstd)
    """
    con.execute(sql)
    con.close()


def benchmark(path: Path, attempts: int = 3) -> dict:
    con = duckdb.connect(database=":memory:")
    # warm-up read to populate OS cache and ensure later timings reflect steady state
    con.execute(SQL, [str(path)]).fetchall()
    timings = []
    result_count = 0
    for _ in range(attempts):
        start = time.perf_counter()
        rows = con.execute(SQL, [str(path)]).fetchall()
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        timings.append(elapsed_ms)
        result_count = len(rows)
    con.close()
    return {
        "rows": path.name.split("_")[-1].split(".")[0],
        "file_mb": round(path.stat().st_size / (1024 * 1024), 2),
        "attempts": attempts,
        "min_ms": round(min(timings), 2),
        "max_ms": round(max(timings), 2),
        "avg_ms": round(sum(timings) / len(timings), 2),
        "result_count": result_count,
    }


def main() -> None:
    targets = [10_000_000, 50_000_000]
    results = []
    for rows in targets:
        path = DATA_DIR / f"sales_{rows}.parquet"
        if not path.exists():
            print(f"Generating {rows} rows -> {path}")
            generate_wide_parquet(path, rows)
        results.append(benchmark(path))
    RESULT_FILE.parent.mkdir(parents=True, exist_ok=True)
    RESULT_FILE.write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
