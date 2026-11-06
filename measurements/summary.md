# Verified measurement facts

## AWS cold start
- Date/time: 2026-10-08T03:10:37Z
- CloudWatch REPORT: Duration 13254.77 ms, Billed Duration 13740 ms, Init Duration 484.39 ms
- Query log: `query.completed` duration 13.249995708465576 seconds, rows=1

## AWS warm path (GET /sales/summary)
- Date/time: 2026-10-08T03:52:06Z to 2026-10-08T03:52:15Z
- Result: 25 sequential requests returned HTTP 200
- Curl `time_total` values in seconds:
  - 2.802442
  - 0.646173
  - 0.583538
  - 2.892664
  - 0.672216
  - 0.593893
  - 0.521492
  - 0.532535
  - 0.620623
  - 0.513792
  - 0.528216
  - 0.576493
  - 0.547348
  - 0.595611
  - 0.584980
  - 0.603054
  - 0.645259
  - 0.511248
  - 0.617700
  - 0.660474
  - 0.630725
  - 0.489550
  - 0.594228
  - 0.694528
  - 0.509592
- CloudWatch query timings after the fix: `query.completed` durations ranged from 0.39076 s to 0.43151 s across warm requests with 20 rows returned.

## Local benchmark numbers
- 10M rows / 140.78 MB -> average 85.06 ms
- 50M rows / 701.25 MB -> average 493.08 ms
