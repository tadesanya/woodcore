How It Works
* _read_transactions checks for required columns, blank identifiers, duplicate transaction IDs, invalid amounts, and invalid timestamps. Amounts become finite numbers; timestamps are parsed as UTC. load_inputs applies the correct schema to each feed.
* reconcile performs an outer merge on transaction_id, so records missing from either feed are preserved. It uses NumPy to classify rows as matched, amount_mismatch, internal_only, or clearing_only. For matched IDs, amount_variance is clearing amount minus internal amount.
* detect_velocity_anomalies runs a query against an in-memory SQLite database. A SQL window function totals absolute outflows per account over a rolling time window. A distinct-sender count uses the same account and an inclusive time interval. By default, an event is flagged when at least 3 senders appear within 10 minutes. The optional outflow rule is enabled with --outflow-threshold.
* build_summary totals the input rows, reconciliation statuses, matched-record variances, unmatched amounts, and anomalies.
* write_reports creates the output directory and writes exceptions, anomalies, and summary CSVs. main connects these steps, prints report paths on success, and returns exit code 2 for handled input or processing errors.

Example:
``` bash
python reconcile.py \
  --internal internal.csv \
  --clearing clearing.csv \
  --outflow-threshold 5000 \
  --outflow-window-minutes 10
```

A couple of details to keep in mind: amount comparison is exact (np.isclose with zero tolerance), and negative amounts represent outflows. Because amounts are converted to floats, integer cents or Decimal would be preferable if the data requires audit-grade monetary precision. Also, the exit code 2 shown for the earlier CLI command indicates a handled error; the stderr message immediately before it identifies the specific cause.