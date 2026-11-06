from __future__ import annotations

import json
import time
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "sample-data" / "benchmark"
RESULT_FILE = ROOT.parent / "measurements" / "raw" / "local_benchmark_more_data.json"

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


def generate_dataset(path: Path, rows: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    countries = ["US", "DE", "FR", "JP", "IN", "BR"]
    categories = ["Electronics", "Home", "Books", "Apparel", "Toys", "Grocery"]

    data = []
    for i in range(1, rows + 1):
        country = countries[(i * 7) % len(countries)]
        category = categories[(i * 11) % len(categories)]
        quantity = (i % 9) + 1
        unit_price = round(((i * 13) % 199) + 5.5, 2)
        data.append(
            {
                "order_id": i,
                "customer_id": (i * 17) % 10000,
                "order_date": pd.Timestamp("2024-01-01") + pd.Timedelta(days=int(i % 365)),
                "country": country,
                "product_category": category,
                "quantity": quantity,
                "unit_price": unit_price,
            }
        )

    df = pd.DataFrame(data)
    con = duckdb.connect(database=":memory:")
    con.register("sales_benchmark", df)
    con.execute("COPY sales_benchmark TO ? (FORMAT parquet)", [str(path)])
    con.close()


def benchmark_file(path: Path, attempts: int = 3) -> dict:
    con = duckdb.connect(database=":memory:")
    durations = []
    result_count = 0

    for _ in range(attempts):
        start = time.perf_counter()
        rows = con.execute(SQL, [str(path)]).fetchall()
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        durations.append(elapsed_ms)
        result_count = len(rows)

    stats = {
        "rows": int(path.stem.split("_")[-1]),
        "file_mb": round(path.stat().st_size / (1024 * 1024), 2),
        "attempts": attempts,
        "min_ms": round(min(durations), 2),
        "max_ms": round(max(durations), 2),
        "avg_ms": round(sum(durations) / len(durations), 2),
        "result_count": result_count,
    }
    con.close()
    return stats


def main() -> None:
    sizes = [100_000, 500_000, 1_000_000]
    results = []

    for rows in sizes:
        path = DATA_DIR / f"sales_{rows}.parquet"
        if not path.exists():
            generate_dataset(path, rows)
        results.append(benchmark_file(path))

    RESULT_FILE.parent.mkdir(parents=True, exist_ok=True)
    RESULT_FILE.write_text(json.dumps(results, indent=2))

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
