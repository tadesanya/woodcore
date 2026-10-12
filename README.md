## Transaction Reconciliation

`reconcile.py` compares internal transaction records with a third-party clearing file, identifies exceptions, and reports account velocity anomalies.

### Requirements

Python 3.10 or newer, NumPy, and Pandas. Install the Python dependencies with:

```sh
python -m pip install -r requirements.txt
```

### Input CSVs

The internal CSV must contain these columns:

| Column | Meaning |
| --- | --- |
| `transaction_id` | Unique transaction key |
| `account_id` | Account receiving or sending funds |
| `sender_id` | Originating sender |
| `timestamp` | Transaction time; timezone-aware values are recommended |
| `amount` | Signed numeric amount; negative values represent outflows |

The clearing CSV must contain `transaction_id` and `amount`. IDs must be unique and nonblank within each file. Amounts must be finite numbers. Timestamps are parsed as UTC; naive timestamps are interpreted as UTC. Amounts are compared exactly, so any nonzero difference is an exception.

### Run

```sh
python reconcile.py --internal internal.csv --clearing clearing.csv --output-dir reports
```

By default, an event is flagged when its account has at least 3 distinct senders in the 10 minutes ending at that event. The time boundary is inclusive. Configure the sender rule with `--sender-threshold` and `--window-minutes`.

High outflow detection is disabled unless `--outflow-threshold` is supplied. It sums the absolute value of negative amounts per account in a rolling window; `--outflow-window-minutes` controls that window and defaults to `--window-minutes`. Events at or above the configured amount are flagged. Both velocity checks use the internal feed.

### Reports

- `reconciliation_exceptions.csv`: amount mismatches and transactions found in only one feed. `amount_variance` is clearing amount minus internal amount for matched IDs.
- `velocity_anomalies.csv`: flagged internal events, distinct sender count, rolling outflow, and the reason(s) each event was flagged.
- `summary.csv`: input counts, reconciliation status counts, signed amount variance total for transactions found in both feeds, unmatched amount totals by feed, and velocity anomaly count.

### Tests

Run the test suite with:

```sh
python -m unittest discover -s tests -v
```

