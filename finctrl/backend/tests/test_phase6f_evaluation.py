import asyncio
import hashlib
import json

import pytest

from finctrl.backend import evaluation_runner as runner


@pytest.fixture(scope="module")
def validation_report():
    return asyncio.run(runner.run_evaluation("validation"))


def test_dataset_split_selection_and_default():
    assert runner._parser().parse_args([]).dataset == "held_out"
    for split in ("dev", "validation", "held_out"):
        dataset, truth, _ = runner.load_split(split)
        assert dataset["metadata"]["dataset_name"] == truth["metadata"]["dataset_name"] == split.upper()
    with pytest.raises(runner.EvaluationIntegrityError): runner.load_split("unknown")


def test_held_out_assets_are_read_only():
    paths = [runner.SPLITS["held_out"] / name for name in ("dataset.json", "ground_truth.json")]
    before = [hashlib.sha256(path.read_bytes()).hexdigest() for path in paths]
    runner.load_split("held_out")
    assert before == [hashlib.sha256(path.read_bytes()).hexdigest() for path in paths]


def test_every_scenario_and_outcome_is_reported(validation_report):
    expected = {"CLEAN_1_TO_1", "CONSOLIDATED_1_TO_N", "FEE_DISCREPANCY", "TRUNCATED_REFERENCE",
                "TIMING_SKEW", "MISSING_RECORD", "CONSOLIDATED_REFUNDS"}
    assert set(validation_report["reconciliation"]["scenarios"]) == expected
    assert set(validation_report["reconciliation"]["expected_outcome_distribution"]) == runner.OUTCOMES
    assert len(validation_report["reconciliation"]["records"]) == 100


@pytest.mark.parametrize("scenario,match_type,expected", [
    ("CLEAN_1_TO_1", "EXACT_1_1", "MATCH"),
    ("CONSOLIDATED_1_TO_N", "CONSOLIDATED", "MATCH_CONSOLIDATED"),
    ("TRUNCATED_REFERENCE", "EXACT_1_1", "MATCH_PARTIAL_REF"),
    ("TIMING_SKEW", "EXACT_1_1", "MATCH_DELAYED"),
    ("CONSOLIDATED_REFUNDS", "CONSOLIDATED", "MATCH_REFUND"),
])
def test_match_normalization(scenario, match_type, expected):
    group = {"scenario": scenario}; ids = {"erp", "pay", "bank"}
    observed, _ = runner._normalize(group, ids, {"matches": [{"type": match_type, "ids": ids}], "candidates": [], "exceptions": []})
    assert observed == expected


@pytest.mark.parametrize("scenario,expected", [("FEE_DISCREPANCY", "MISMATCH_FEE"), ("MISSING_RECORD", "MISSING_DATA")])
def test_controlled_exception_normalization(scenario, expected):
    observed, _ = runner._normalize({"scenario": scenario}, {"erp"}, {"matches": [], "candidates": [], "exceptions": [{"type": "x", "ids": {"erp"}}]})
    assert observed == expected


def test_metrics_confusion_and_per_scenario_accuracy(validation_report):
    reconciliation = validation_report["reconciliation"]
    assert reconciliation["overall"]["groups"] == 100
    assert reconciliation["confusion_matrix"]
    assert all("accuracy" in value for value in reconciliation["scenarios"].values())
    assert reconciliation["precision"] is not None and reconciliation["recall"] is not None


def test_financial_duplicate_razorpay_ai_and_forecast_invariants(validation_report):
    assert validation_report["financial_invariants"]["financial_immutability"]["status"] == "PASS"
    assert validation_report["financial_invariants"]["idempotency"]["status"] == "PASS"
    assert validation_report["razorpay"]["status"] == "PASS"
    assert validation_report["ai_safety"]["status"] == "PASS"
    assert validation_report["forecasting"]["status"] == "PASS"


def test_evaluation_is_deterministic_with_stable_digest(validation_report):
    repeated = asyncio.run(runner.run_evaluation("validation"))
    assert validation_report["evaluation_digest"] == repeated["evaluation_digest"]
    assert validation_report["reconciliation"] == repeated["reconciliation"]


def test_unavailable_frontend_check_is_not_silently_passed(validation_report):
    assert validation_report["frontend"]["status"] == "NOT_APPLICABLE"
    assert validation_report["production_readiness"]["accuracy_threshold"].startswith("NOT_APPLICABLE")


def test_corrupt_dataset_is_rejected(monkeypatch, tmp_path):
    split = tmp_path / "held_out"; split.mkdir(); (split / "dataset.json").write_text("{"); (split / "ground_truth.json").write_text("{}")
    monkeypatch.setitem(runner.SPLITS, "held_out", split)
    with pytest.raises(runner.EvaluationIntegrityError): runner.load_split("held_out")


def test_cli_success_and_failure_exit_codes(monkeypatch, tmp_path):
    async def passed(*args, **kwargs):
        return {"reconciliation": {"overall": {"correct": 1, "groups": 1}}, "evaluation_digest": "abc",
                "production_readiness": {"status": "PASS"}}
    monkeypatch.setattr(runner, "run_evaluation", passed)
    assert runner.main(["--dataset", "held_out", "--output", str(tmp_path / "report.json")]) == 0
    async def failed(*args, **kwargs): raise runner.EvaluationIntegrityError("bad")
    monkeypatch.setattr(runner, "run_evaluation", failed)
    assert runner.main(["--dataset", "held_out"]) == 2
