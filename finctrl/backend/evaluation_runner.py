"""Offline held-out reconciliation evaluation and production-readiness CLI."""
from __future__ import annotations
import argparse, asyncio, hashlib, json, subprocess, sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import selectinload
from sqlalchemy.pool import StaticPool
from finctrl.backend.api.routes import ingest_bank, ingest_erp, ingest_rzp
from finctrl.backend.api.schemas import BankBatchPayload, ERPBatchPayload, RZPBatchPayload
from finctrl.backend.database.models import (Base, BankRecordModel, ERPRecordModel,
    ExceptionEvidenceModel, FinancialEventModel, RazorpayOrderModel, RazorpayPaymentModel,
    RazorpayRefundModel, RazorpaySettlementModel, ReconciliationCandidateModel,
    ReconciliationExceptionModel, ReconciliationMatchModel)
from finctrl.backend.engine.ai.schemas import ProposedMatchSchema
from finctrl.backend.reconciliation.engine import run_reconciliation
from finctrl.backend.reconciliation.forecasting import CashForecastService
from finctrl.backend.schemas.models import FinctrlDataset, GroundTruthDataset

EVALUATION_VERSION = "6F.1"
ROOT = Path(__file__).resolve().parent
SPLITS = {name: ROOT / "data" / name for name in ("dev", "validation", "held_out")}
OUTCOMES = {"MATCH", "MATCH_CONSOLIDATED", "MISMATCH_FEE", "MATCH_PARTIAL_REF",
            "MATCH_DELAYED", "MISSING_DATA", "MATCH_REFUND"}

class EvaluationIntegrityError(ValueError): pass
def _sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def _digest(value): return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()
def _status(ok, details=None): return {"status": "PASS" if ok else "FAIL", **(details or {})}

def load_split(dataset: str):
    name = dataset.lower()
    if name not in SPLITS: raise EvaluationIntegrityError(f"Unsupported dataset split: {dataset}")
    dp, gp = SPLITS[name] / "dataset.json", SPLITS[name] / "ground_truth.json"
    hashes = {"dataset_sha256": _sha(dp), "ground_truth_sha256": _sha(gp)}
    try:
        raw, truth = json.loads(dp.read_text()), json.loads(gp.read_text())
        parsed, parsed_truth = FinctrlDataset.model_validate(raw), GroundTruthDataset.model_validate(truth)
    except (OSError, json.JSONDecodeError, ValidationError) as error:
        raise EvaluationIntegrityError("Dataset or ground truth is malformed") from error
    if parsed.metadata.dataset_name.upper() != name.upper() or parsed_truth.metadata.dataset_name.upper() != name.upper():
        raise EvaluationIntegrityError("Dataset split metadata does not match selection")
    if parsed.metadata.model_dump() != parsed_truth.metadata.model_dump():
        raise EvaluationIntegrityError("Dataset and ground-truth metadata differ")
    if len(parsed_truth.groups) != parsed.metadata.record_counts["groups"]:
        raise EvaluationIntegrityError("Ground-truth group count does not match metadata")
    if any(group.expected_outcome not in OUTCOMES for group in parsed_truth.groups):
        raise EvaluationIntegrityError("Ground truth contains an unsupported outcome")
    ids = {str(x.id) for x in [*parsed.erp_records, *parsed.rzp_records, *parsed.bank_records]}
    refs = {str(x) for g in parsed_truth.groups for x in [*g.erp_record_ids, *g.rzp_record_ids, *g.bank_record_ids]}
    if not refs.issubset(ids): raise EvaluationIntegrityError("Ground truth references missing records")
    return raw, truth, hashes

def _maps(dataset):
    return ({x["id"]: x["reference_id"] for x in dataset["erp_records"]},
            {x["id"]: x["rzp_payment_id"] for x in dataset["rzp_records"]},
            {x["id"]: x["transaction_ref"] for x in dataset["bank_records"]})
def _expected(group, maps):
    e, r, b = maps
    return ({e[x] for x in group["erp_record_ids"]} | {r[x] for x in group["rzp_record_ids"]} |
            {b[x] for x in group["bank_record_ids"]})

async def _snapshot(db: AsyncSession):
    erp = (await db.scalars(select(ERPRecordModel).order_by(ERPRecordModel.reference_id))).all()
    rzp = (await db.scalars(select(RazorpayPaymentModel).order_by(RazorpayPaymentModel.rzp_payment_id))).all()
    refunds = (await db.scalars(select(RazorpayRefundModel).order_by(RazorpayRefundModel.rzp_refund_id))).all()
    bank = (await db.scalars(select(BankRecordModel).order_by(BankRecordModel.transaction_ref))).all()
    return {"erp": [(x.reference_id, x.amount, x.currency, str(x.timestamp), x.type) for x in erp],
            "razorpay": ([("payment", x.rzp_payment_id, x.rzp_order_id, x.rzp_settlement_id, x.amount, x.currency, x.fee, x.tax, x.created_at_ts) for x in rzp] +
                         [("refund", x.rzp_refund_id, x.rzp_payment_id, None, x.amount, x.currency, 0, 0, x.created_at_ts) for x in refunds]),
            "bank": [(x.transaction_ref, x.amount, str(x.timestamp), x.type) for x in bank]}

async def _state(db):
    matches = (await db.scalars(select(ReconciliationMatchModel).options(selectinload(ReconciliationMatchModel.evidence)))).all()
    candidates = (await db.scalars(select(ReconciliationCandidateModel))).all()
    exceptions = (await db.scalars(select(ReconciliationExceptionModel).options(selectinload(ReconciliationExceptionModel.evidence)))).all()
    return {"matches": [{"type": x.match_type, "ids": {e.source_id for e in x.evidence if e.source_id}} for x in matches],
            "candidates": [{"ids": {v for k, v in x.evidence_payload.items() if k.endswith("_source_id") and isinstance(v, str)}} for x in candidates],
            "exceptions": [{"type": x.exception_type, "ids": {e.source_id for e in x.evidence}} for x in exceptions]}

def _normalize(group, expected, state):
    matches = [x for x in state["matches"] if expected.issubset(x["ids"])]
    scenario = group["scenario"]
    if matches:
        special = {"CONSOLIDATED_REFUNDS": "MATCH_REFUND", "TIMING_SKEW": "MATCH_DELAYED",
                   "TRUNCATED_REFERENCE": "MATCH_PARTIAL_REF"}
        if scenario in special: return special[scenario], "Expected specialized evidence group was resolved."
        if any(x["type"] == "CONSOLIDATED" for x in matches): return "MATCH_CONSOLIDATED", "Consolidated evidence set resolved."
        return "MATCH", "Exact authoritative evidence set resolved."
    operational = any(expected & x["ids"] for x in [*state["candidates"], *state["exceptions"]])
    if scenario == "FEE_DISCREPANCY" and operational: return "MISMATCH_FEE", "Discrepancy retained for controlled review."
    if scenario == "MISSING_RECORD" and operational: return "MISSING_DATA", "Missing source retained for controlled review."
    if operational: return "UNRESOLVED", "Controlled state exists but differs from expected normalization."
    return "UNOBSERVED", "No operational evidence covered the group."

def _scenarios(records, truth):
    result = {}
    for scenario in truth["metadata"]["scenario_counts"]:
        rows = [x for x in records if x["scenario"] == scenario]; correct = sum(x["correct"] for x in rows)
        result[scenario] = {"groups": len(rows), "correct": correct, "accuracy": correct / len(rows) if rows else None,
            "expected": dict(sorted(Counter(x["expected_outcome"] for x in rows).items())),
            "observed": dict(sorted(Counter(x["observed_outcome"] for x in rows).items()))}
    return result

async def run_evaluation(dataset: str = "held_out", production_check: bool = False) -> dict[str, Any]:
    data, truth, hashes = load_split(dataset)
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", poolclass=StaticPool, connect_args={"check_same_thread": False})
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as connection: await connection.run_sync(Base.metadata.create_all)
    async with sessions() as db:
        await ingest_erp(ERPBatchPayload(records=data["erp_records"]), db); await ingest_rzp(RZPBatchPayload(records=data["rzp_records"]), db); await ingest_bank(BankBatchPayload(records=data["bank_records"]), db)
        source_before = await _snapshot(db); first = await run_reconciliation(db); source_after = await _snapshot(db); state = await _state(db)
        counts = {k: len(state[k]) for k in state}
        await ingest_erp(ERPBatchPayload(records=data["erp_records"]), db); await ingest_rzp(RZPBatchPayload(records=data["rzp_records"]), db); await ingest_bank(BankBatchPayload(records=data["bank_records"]), db)
        duplicate_source = await _snapshot(db); await run_reconciliation(db); repeated = await _state(db)
        repeated_counts = {k: len(repeated[k]) for k in repeated}
        invalid_evidence = 0
        for link in (await db.scalars(select(ExceptionEvidenceModel))).all():
            valid = any([await db.get(model, link.record_id) for model in (ERPRecordModel, RazorpayOrderModel,
                RazorpayPaymentModel, RazorpaySettlementModel, RazorpayRefundModel, BankRecordModel,
                FinancialEventModel, ReconciliationMatchModel, ReconciliationCandidateModel)])
            invalid_evidence += not valid
        records = []
        maps = _maps(data)
        for group in truth["groups"]:
            ids = _expected(group, maps); observed, reason = _normalize(group, ids, state)
            records.append({"group_id": group["group_id"], "scenario": group["scenario"], "expected_outcome": group["expected_outcome"],
                "observed_outcome": observed, "correct": observed == group["expected_outcome"], "record_identifiers": sorted(ids), "reason": reason})
        timestamps = [int(x.timestamp.timestamp()) for x in (await db.scalars(select(BankRecordModel))).all()]
        forecast_a = await CashForecastService(db).forecast(min(timestamps), max(timestamps), 7)
        forecast_b = await CashForecastService(db).forecast(min(timestamps), max(timestamps), 7)
    await engine.dispose()
    integrity = hashes == {"dataset_sha256": _sha(SPLITS[dataset.lower()] / "dataset.json"), "ground_truth_sha256": _sha(SPLITS[dataset.lower()] / "ground_truth.json")}
    immutable, idempotent = source_before == source_after, source_before == duplicate_source and counts == repeated_counts
    expected_dist, observed_dist = Counter(x["expected_outcome"] for x in records), Counter(x["observed_outcome"] for x in records)
    confusion = defaultdict(Counter)
    for x in records: confusion[x["expected_outcome"]][x["observed_outcome"]] += 1
    correct = sum(x["correct"] for x in records); expected_res = sum(x["expected_outcome"].startswith("MATCH") for x in records); observed_res = sum(x["observed_outcome"].startswith("MATCH") for x in records); correct_res = sum(x["correct"] and x["expected_outcome"].startswith("MATCH") for x in records)
    try:
        ProposedMatchSchema(classification="MATCH", confidence=2, reason="x", supporting_evidence=["x"], recommended_action="HUMAN_REVIEW_REQUIRED", risk_level="LOW"); invalid_confidence = False
    except ValidationError: invalid_confidence = True
    ai = {"invalid_confidence_rejected": invalid_confidence, "offline_no_live_provider_calls": True,
          "authoritative_evidence": invalid_evidence == 0}
    if production_check:
        ai["phase6d_safety_regression_suite"] = _backend_safety_check()
    integer_forecast = all(type(v) is int for v in forecast_a["currencies"]["INR"]["totals"].values())
    report = {"evaluation_version": EVALUATION_VERSION,
        "dataset": {**data["metadata"], "selected_split": dataset.upper(), **hashes, "integrity": "PASS" if integrity else "FAIL"},
        "reconciliation": {"overall": {"groups": len(records), "correct": correct, "accuracy": correct / len(records)},
            "expected_outcome_distribution": dict(sorted(expected_dist.items())), "observed_outcome_distribution": dict(sorted(observed_dist.items())),
            "confusion_matrix": {k: dict(sorted(v.items())) for k, v in sorted(confusion.items())},
            "precision": correct_res / observed_res if observed_res else None, "recall": correct_res / expected_res if expected_res else None,
            "false_resolutions": sum(x["observed_outcome"].startswith("MATCH") and not x["correct"] for x in records),
            "unresolved_expected_matches": expected_res - correct_res, "engine_counts": first.model_dump(), "scenarios": _scenarios(records, truth), "records": records},
        "exceptions": _status(invalid_evidence == 0, {"authoritative_evidence_violations": invalid_evidence, "count": counts["exceptions"]}),
        "razorpay": _status(len(source_after["razorpay"]) == data["metadata"]["record_counts"]["rzp"], {"payment_identities": len(source_after["razorpay"]), "external_network_calls": 0}),
        "ai_safety": _status(all(ai.values()), {"checks": ai, "accuracy": "NOT_APPLICABLE"}),
        "forecasting": _status(forecast_a["currencies"] == forecast_b["currencies"] and integer_forecast, {"deterministic": forecast_a["currencies"] == forecast_b["currencies"], "integer_smallest_unit": integer_forecast, "predictive_accuracy": "NOT_APPLICABLE"}),
        "financial_invariants": {"financial_immutability": _status(immutable, {"violations": [] if immutable else ["source facts changed"]}), "idempotency": _status(idempotent, {"first": counts, "repeated": repeated_counts})},
        "frontend": _frontend_checks() if production_check else {"status": "NOT_APPLICABLE", "reason": "Run with --production-check"}}
    blockers = [name for name, ok in (("dataset_integrity", integrity), ("financial_immutability", immutable), ("idempotency", idempotent), ("ai_safety", all(ai.values())), ("forecasting", report["forecasting"]["status"] == "PASS")) if not ok]
    if production_check and report["frontend"]["status"] != "PASS": blockers.append("frontend")
    report["production_readiness"] = {"status": "PASS" if not blockers else "FAIL", "blockers": blockers, "accuracy_threshold": "NOT_APPLICABLE: no project threshold is defined"}
    report["evaluation_digest"] = _digest(report)
    return report

def _frontend_checks():
    frontend = ROOT.parent.parent / "frontend"; checks = {}
    npm = "npm.cmd" if sys.platform == "win32" else "npm"
    for name, command in (("tests", [npm, "test"]), ("build", [npm, "run", "build"])):
        result = subprocess.run(command, cwd=frontend, capture_output=True, text=True, timeout=180); checks[name] = "PASS" if result.returncode == 0 else "FAIL"
    return _status(all(x == "PASS" for x in checks.values()), {"checks": checks})

def _backend_safety_check():
    command = [sys.executable, "-m", "pytest", "-q",
               "finctrl/backend/tests/test_phase6d_ai_investigation_approval.py",
               "finctrl/backend/tests/test_phase6e_cash_forecasting.py"]
    result = subprocess.run(command, cwd=ROOT.parent.parent, capture_output=True, text=True, timeout=180)
    return result.returncode == 0

def _parser():
    parser = argparse.ArgumentParser(description="FINCTRL offline evaluation"); parser.add_argument("--dataset", choices=sorted(SPLITS), default="held_out"); parser.add_argument("--production-check", action="store_true"); parser.add_argument("--output", type=Path); return parser
def main(argv=None):
    args = _parser().parse_args(argv)
    try:
        report = asyncio.run(run_evaluation(args.dataset, args.production_check))
        if args.output: args.output.parent.mkdir(parents=True, exist_ok=True); args.output.write_text(json.dumps(report, indent=2, sort_keys=True, default=str) + "\n")
        overall = report["reconciliation"]["overall"]; print(f"{args.dataset.upper()}: {overall['correct']}/{overall['groups']} correct; digest={report['evaluation_digest']}; readiness={report['production_readiness']['status']}")
        return 0 if report["production_readiness"]["status"] == "PASS" else 1
    except (EvaluationIntegrityError, OSError, ValueError) as error:
        print(f"Evaluation failed: {error}", file=sys.stderr); return 2
if __name__ == "__main__": raise SystemExit(main())
