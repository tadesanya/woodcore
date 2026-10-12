import tempfile
import unittest
from pathlib import Path
from contextlib import redirect_stdout
from io import StringIO

import pandas as pd

from reconcile import (
    build_summary,
    detect_velocity_anomalies,
    load_inputs,
    main,
    reconcile,
    write_reports,
)


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary_directory.name)
        self.internal_path = self.directory / "internal.csv"
        self.clearing_path = self.directory / "clearing.csv"
        pd.DataFrame(
            [
                ["t1", "acct-a", "sender-1", "2026-01-01T00:00:00Z", -10],
                ["t2", "acct-a", "sender-2", "2026-01-01T00:05:00Z", -20],
                ["t3", "acct-a", "sender-3", "2026-01-01T00:10:00Z", -30],
                ["t4", "acct-b", "sender-b", "2026-01-01T00:05:00Z", 12],
            ],
            columns=["transaction_id", "account_id", "sender_id", "timestamp", "amount"],
        ).to_csv(self.internal_path, index=False)
        pd.DataFrame(
            [["t1", -10], ["t2", -21], ["t5", 4]],
            columns=["transaction_id", "amount"],
        ).to_csv(self.clearing_path, index=False)

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_reconciliation_classifies_matches_mismatches_and_unmatched(self):
        internal, clearing = load_inputs(self.internal_path, self.clearing_path)

        result = reconcile(internal, clearing).set_index("transaction_id")

        self.assertEqual(result.loc["t1", "status"], "matched")
        self.assertEqual(result.loc["t2", "status"], "amount_mismatch")
        self.assertEqual(result.loc["t2", "amount_variance"], -1)
        self.assertEqual(result.loc["t3", "status"], "internal_only")
        self.assertEqual(result.loc["t5", "status"], "clearing_only")

    def test_rejects_duplicate_transaction_ids(self):
        duplicate = pd.read_csv(self.internal_path)
        pd.concat([duplicate, duplicate.iloc[[0]]]).to_csv(self.internal_path, index=False)

        with self.assertRaisesRegex(ValueError, "duplicate transaction_id"):
            load_inputs(self.internal_path, self.clearing_path)

    def test_rejects_invalid_amount_and_timestamp(self):
        invalid = pd.read_csv(self.internal_path, dtype={"amount": "string"})
        invalid.loc[0, "amount"] = "inf"
        invalid.to_csv(self.internal_path, index=False)
        with self.assertRaisesRegex(ValueError, "finite numbers"):
            load_inputs(self.internal_path, self.clearing_path)

        invalid.loc[0, "amount"] = "-10"
        invalid.loc[0, "timestamp"] = "not-a-timestamp"
        invalid.to_csv(self.internal_path, index=False)
        with self.assertRaisesRegex(ValueError, "invalid values"):
            load_inputs(self.internal_path, self.clearing_path)

    def test_sender_velocity_is_account_scoped_and_window_inclusive(self):
        internal, _ = load_inputs(self.internal_path, self.clearing_path)

        anomalies = detect_velocity_anomalies(internal)

        self.assertEqual(anomalies["transaction_id"].tolist(), ["t3"])
        self.assertEqual(anomalies.iloc[0]["distinct_senders"], 3)

    def test_configured_outflow_threshold_flags_rolling_outflow(self):
        internal, _ = load_inputs(self.internal_path, self.clearing_path)

        anomalies = detect_velocity_anomalies(internal, outflow_threshold=25)
        reasons = anomalies.set_index("transaction_id")["anomaly_reasons"]

        self.assertEqual(reasons.loc["t2"], "high_outflow")
        self.assertEqual(reasons.loc["t3"], "multiple_senders;high_outflow")
        with self.assertRaisesRegex(ValueError, "outflow window"):
            detect_velocity_anomalies(internal, outflow_threshold=25, outflow_window_minutes=0)

    def test_writes_exception_anomaly_and_summary_reports(self):
        internal, clearing = load_inputs(self.internal_path, self.clearing_path)
        results = reconcile(internal, clearing)
        anomalies = detect_velocity_anomalies(internal)
        summary = build_summary(internal, clearing, results, anomalies)

        reports = write_reports(self.directory / "reports", results, anomalies, summary)

        self.assertEqual(set(reports), {"exceptions", "anomalies", "summary"})
        self.assertTrue(all(path.exists() for path in reports.values()))
        exceptions = pd.read_csv(reports["exceptions"])
        self.assertEqual(set(exceptions["status"]), {"amount_mismatch", "internal_only", "clearing_only"})
        saved_summary = pd.read_csv(reports["summary"]).iloc[0]
        self.assertEqual(saved_summary["matched_count"], 1)
        self.assertEqual(saved_summary["amount_mismatch_count"], 1)
        self.assertEqual(saved_summary["velocity_anomaly_count"], 1)

    def test_cli_writes_reports_and_accepts_optional_outflow_rule(self):
        output_directory = self.directory / "cli-reports"
        output = StringIO()

        with redirect_stdout(output):
            exit_code = main(
                [
                    "--internal", str(self.internal_path),
                    "--clearing", str(self.clearing_path),
                    "--output-dir", str(output_directory),
                    "--outflow-threshold", "25",
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertIn("velocity anomalies: 2", output.getvalue())
        self.assertTrue((output_directory / "reconciliation_exceptions.csv").exists())
        self.assertTrue((output_directory / "velocity_anomalies.csv").exists())
        self.assertTrue((output_directory / "summary.csv").exists())


if __name__ == "__main__":
    unittest.main()