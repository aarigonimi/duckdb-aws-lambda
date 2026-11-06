# DuckDB on AWS Lambda

A serverless analytics sample that queries Parquet data in Amazon S3 using DuckDB inside an AWS Lambda container image.

This project demonstrates a practical pattern for running analytical SQL workloads on cold-start-friendly serverless infrastructure without moving the full dataset into a traditional database.

## Architecture

- AWS Lambda runs the ingestion and query logic.
- DuckDB loads the `httpfs`, `aws`, and `json` extensions.
- The Lambda function reads Parquet files from S3 using DuckDB's S3 support.
- Aggregation results are written back to S3 as Parquet and JSON artifacts.
- CloudWatch captures logs, metrics, and request timings.

## Why this project

The repository is designed to show how to:

- query large Parquet files directly in S3,
- keep the processing layer stateless and serverless,
- run analytical SQL at lower cost for read-heavy workloads,
- package Python + DuckDB inside a Lambda container image.

## Repository layout

- `src/lambda_function.py` — Lambda handler and DuckDB logic
- `src/tests/test_lambda_function.py` — unit tests
- `docker/Dockerfile` — Lambda container image build
- `scripts/build.sh` — Docker build helper
- `measurements/summary.md` — benchmark notes and timings
- `docs/` — project documentation
- `terraform/` — infrastructure definitions for deployment

## Prerequisites

- Docker
- Python 3.11+
- AWS credentials configured for local development or deployment
- An S3 bucket containing Parquet files under a `raw/` prefix

## Local setup

```bash
cd duckdb-aws-lambda
python -m venv .venv
source .venv/bin/activate
pip install -r src/requirements-dev.txt
```

## Run tests

```bash
pytest -q
```

## Build the Lambda container image

```bash
./scripts/build.sh
```

## Example environment

Copy the example file and adjust values as needed:

```bash
cp .env.example .env
```

Typical values include:

- `AWS_REGION`
- `SALES_BUCKET`
- `SALES_PREFIX`
- `RAW_PREFIX`
- `RESULTS_PREFIX`

## Notes

The implementation is intentionally conservative:

- only raw parquet files under the expected prefix are processed,
- output objects reuse deterministic keys to avoid duplicate writes on retries,
- Lambda uses the AWS credential chain instead of hard-coded secrets.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
