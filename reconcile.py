"""Reconcile transaction feeds and report account velocity anomalies."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd


INTERNAL_COLUMNS = ["transaction_id", "account_id", "sender_id", "timestamp", "amount"]
CLEARING_COLUMNS = ["transaction_id", "amount"]
REPORT_NAMES = {
    "exceptions": "reconciliation_exceptions.csv",
    "anomalies": "velocity_anomalies.csv",
    "summary": "summary.csv",
}


def _read_transactions(path: Path, required_columns: list[str], feed_name: str) -> pd.DataFrame:
    try:
        frame = pd.read_csv(path, dtype={column: "string" for column in required_columns if column != "amount"})
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError) as error:
        raise ValueError(f"Could not read {feed_name} CSV '{path}': {error}") from error

    missing_columns = sorted(set(required_columns) - set(frame.columns))
    if missing_columns:
        raise ValueError(f"{feed_name} CSV is missing required columns: {', '.join(missing_columns)}")

    frame = frame[required_columns].copy()
    for column in required_columns:
        if column == "amount":
            continue
        frame[column] = frame[column].str.strip()
        invalid_ids = frame[column].isna() | frame[column].eq("")
        if invalid_ids.any():
            raise ValueError(f"{feed_name} CSV column '{column}' contains blank values")

    if frame["transaction_id"].duplicated().any():
        raise ValueError(f"{feed_name} CSV contains duplicate transaction_id values")

    amount = pd.to_numeric(frame["amount"], errors="coerce")
    if amount.isna().any() or not np.isfinite(amount.to_numpy(dtype=float)).all():
        raise ValueError(f"{feed_name} CSV column 'amount' must contain finite numbers")
    frame["amount"] = amount.astype(float)

    if "timestamp" in required_columns:
        timestamps = pd.to_datetime(frame["timestamp"], errors="coerce", utc=True)
        if timestamps.isna().any():
            raise ValueError(f"{feed_name} CSV column 'timestamp' contains invalid values")
        frame["timestamp"] = timestamps

    return frame


def load_inputs(internal_path: Path, clearing_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read and validate internal and clearing transaction CSV files."""
    internal = _read_transactions(internal_path, INTERNAL_COLUMNS, "Internal")
    clearing = _read_transactions(clearing_path, CLEARING_COLUMNS, "Clearing")
    return internal, clearing


def reconcile(internal: pd.DataFrame, clearing: pd.DataFrame) -> pd.DataFrame:
    """Return every transaction with a reconciliation status and signed variance."""
    merged = internal.merge(
        clearing,
        on="transaction_id",
        how="outer",
        suffixes=("_internal", "_clearing"),
        indicator=True,
        validate="one_to_one",
    )

    is_matched = merged["_merge"].eq("both").to_numpy()
    internal_amount = merged["amount_internal"].fillna(0).to_numpy(dtype=float)
    clearing_amount = merged["amount_clearing"].fillna(0).to_numpy(dtype=float)
    amounts_match = np.isclose(internal_amount, clearing_amount, rtol=0, atol=0) & is_matched

    merged["status"] = np.select(
        [merged["_merge"].eq("left_only"), merged["_merge"].eq("right_only"), amounts_match],
        ["internal_only", "clearing_only", "matched"],
        default="amount_mismatch",
    )
    merged["amount_variance"] = np.where(
        is_matched,
        clearing_amount - internal_amount,
        np.nan,
    )
    return merged.drop(columns="_merge")


def detect_velocity_anomalies(
    internal: pd.DataFrame,
    sender_threshold: int = 3,
    window_minutes: int = 10,
    outflow_threshold: float | None = None,
    outflow_window_minutes: int | None = None,
) -> pd.DataFrame:
    """Flag account events with multiple senders or configured rapid outflows."""
    if sender_threshold < 1:
        raise ValueError("sender threshold must be at least 1")
    if window_minutes < 1:
        raise ValueError("sender window must be at least 1 minute")
    if outflow_threshold is not None and (
        not np.isfinite(outflow_threshold) or outflow_threshold <= 0
    ):
        raise ValueError("outflow threshold must be greater than 0")

    if outflow_window_minutes is None:
        outflow_window_minutes = window_minutes
    if outflow_window_minutes < 1:
        raise ValueError("outflow window must be at least 1 minute")

    columns = ["transaction_id", "account_id", "sender_id", "timestamp", "amount"]
    if internal.empty:
        return pd.DataFrame(columns=columns + ["distinct_senders", "rolling_outflow", "anomaly_reasons"])

    events = internal[columns].copy().reset_index(drop=True)
    events["event_id"] = np.arange(len(events), dtype=int)
    events["event_epoch"] = events["timestamp"].astype("int64") / 1_000_000_000

    sender_window_seconds = window_minutes * 60
    outflow_window_seconds = outflow_window_minutes * 60

    with sqlite3.connect(":memory:") as connection:
        events[["event_id", "account_id", "sender_id", "amount", "event_epoch"]].to_sql(
            "events", connection, index=False
        )
        query = """
            WITH metrics AS (
                SELECT
                    event_id,
                    account_id,
                    sender_id,
                    amount,
                    event_epoch,
                    SUM(CASE WHEN amount < 0 THEN -amount ELSE 0 END) OVER (
                        PARTITION BY account_id
                        ORDER BY event_epoch
                        RANGE BETWEEN ? PRECEDING AND CURRENT ROW
                    ) AS rolling_outflow
                FROM events
            )
            SELECT
                current.event_id,
                COUNT(DISTINCT prior.sender_id) AS distinct_senders,
                current.rolling_outflow
            FROM metrics AS current
            JOIN events AS prior
              ON prior.account_id = current.account_id
             AND prior.event_epoch BETWEEN current.event_epoch - ? AND current.event_epoch
            GROUP BY current.event_id, current.rolling_outflow
            ORDER BY current.event_id
        """
        metrics = pd.read_sql_query(
            query,
            connection,
            params=(outflow_window_seconds, sender_window_seconds),
        )

    events = events.merge(metrics, on="event_id", validate="one_to_one")
    sender_anomaly = events["distinct_senders"].ge(sender_threshold)
    reasons = np.where(sender_anomaly, "multiple_senders", "")
    if outflow_threshold is not None:
        outflow_anomaly = events["rolling_outflow"].ge(outflow_threshold)
        reasons = np.where(
            outflow_anomaly,
            np.where(reasons == "", "high_outflow", np.char.add(reasons, ";high_outflow")),
            reasons,
        )

    events["anomaly_reasons"] = reasons
    return events.loc[events["anomaly_reasons"].ne("")].drop(
        columns=["event_id", "event_epoch"]
    ).reset_index(drop=True)


def build_summary(
    internal: pd.DataFrame,
    clearing: pd.DataFrame,
    results: pd.DataFrame,
    anomalies: pd.DataFrame,
) -> pd.DataFrame:
    counts = results["status"].value_counts()
    matched = results.loc[results["status"].isin(["matched", "amount_mismatch"])]
    return pd.DataFrame(
        [
            {
                "internal_count": len(internal),
                "clearing_count": len(clearing),
                "matched_count": int(counts.get("matched", 0)),
                "amount_mismatch_count": int(counts.get("amount_mismatch", 0)),
                "internal_only_count": int(counts.get("internal_only", 0)),
                "clearing_only_count": int(counts.get("clearing_only", 0)),
                "signed_matched_variance_total": float(matched["amount_variance"].sum()),
                "internal_only_amount_total": float(
                    results.loc[results["status"].eq("internal_only"), "amount_internal"].sum()
                ),
                "clearing_only_amount_total": float(
                    results.loc[results["status"].eq("clearing_only"), "amount_clearing"].sum()
                ),
                "velocity_anomaly_count": len(anomalies),
            }
        ]
    )


def write_reports(
    output_directory: Path,
    results: pd.DataFrame,
    anomalies: pd.DataFrame,
    summary: pd.DataFrame,
) -> dict[str, Path]:
    output_directory.mkdir(parents=True, exist_ok=True)
    reports = {name: output_directory / filename for name, filename in REPORT_NAMES.items()}
    results.loc[results["status"].ne("matched")].to_csv(reports["exceptions"], index=False)
    anomalies.to_csv(reports["anomalies"], index=False)
    summary.to_csv(reports["summary"], index=False)
    return reports


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconcile internal and clearing transaction CSVs.")
    parser.add_argument("--internal", required=True, type=Path, help="Internal transaction CSV")
    parser.add_argument("--clearing", required=True, type=Path, help="Third-party clearing CSV")
    parser.add_argument("--output-dir", type=Path, default=Path("reports"), help="Report output directory")
    parser.add_argument("--sender-threshold", type=int, default=3, help="Distinct senders needed to flag an account")
    parser.add_argument("--window-minutes", type=int, default=10, help="Sender-count rolling window")
    parser.add_argument("--outflow-threshold", type=float, help="Optional rolling absolute-outflow threshold")
    parser.add_argument("--outflow-window-minutes", type=int, help="Outflow window; defaults to --window-minutes")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        internal, clearing = load_inputs(args.internal, args.clearing)
        results = reconcile(internal, clearing)
        anomalies = detect_velocity_anomalies(
            internal,
            sender_threshold=args.sender_threshold,
            window_minutes=args.window_minutes,
            outflow_threshold=args.outflow_threshold,
            outflow_window_minutes=args.outflow_window_minutes,
        )
        summary = build_summary(internal, clearing, results, anomalies)
        reports = write_reports(args.output_dir, results, anomalies, summary)
    except (OSError, ValueError, sqlite3.Error) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 2

    for name, path in reports.items():
        print(f"{name}: {path}")
    print(f"transactions: {len(results)}; exceptions: {int(results['status'].ne('matched').sum())}; "
          f"velocity anomalies: {len(anomalies)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())