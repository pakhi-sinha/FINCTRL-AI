import pytest
from finctrl.backend.engine.policy import evaluate_policy
from finctrl.backend.engine.ai.schemas import ProposedMatchSchema
from finctrl.backend.engine.ai.evidence import EvidencePackage

def test_hallucinated_evidence_id():
    proposal = ProposedMatchSchema(
        decision="PROPOSE_MATCH",
        match_type="ONE_TO_ONE",
        evidence_ids=["fake-id"],
        confidence=0.99,
        reasoning="test"
    )
    evidence = EvidencePackage(candidate={})
    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "REJECTED"
    assert not decision.is_valid

def test_amount_only_match_rejected():
    proposal = ProposedMatchSchema(
        decision="PROPOSE_MATCH",
        match_type="ONE_TO_ONE",
        evidence_ids=["erp-1", "rzp-1"],
        confidence=0.99,
        reasoning="test"
    )
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "refA"}]
    evidence.rzp_records = [{"id": "rzp-1", "gross_amount": 100, "order_receipt": "refB"}]

    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "REJECTED"
    assert "Amount-only match is forbidden" in decision.reason

def test_auto_resolve_valid():
    proposal = ProposedMatchSchema(
        decision="PROPOSE_MATCH",
        match_type="ONE_TO_ONE",
        evidence_ids=["erp-1", "rzp-1"],
        confidence=0.96,
        reasoning="test"
    )
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "refA"}]
    evidence.rzp_records = [{"id": "rzp-1", "gross_amount": 100, "order_receipt": "refA"}]

    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "AUTO_RESOLVE"
    assert decision.is_valid

def test_human_review_required():
    proposal = ProposedMatchSchema(
        decision="PROPOSE_MATCH",
        match_type="ONE_TO_ONE",
        evidence_ids=["erp-1", "rzp-1"],
        confidence=0.85,
        reasoning="test"
    )
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "refA"}]
    evidence.rzp_records = [{"id": "rzp-1", "gross_amount": 100, "order_receipt": "refA"}]

    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "HUMAN_REVIEW_REQUIRED"
    assert decision.is_valid

def test_exception_low_confidence():
    proposal = ProposedMatchSchema(
        decision="PROPOSE_MATCH",
        match_type="ONE_TO_ONE",
        evidence_ids=["erp-1", "rzp-1"],
        confidence=0.60,
        reasoning="test"
    )
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "refA"}]
    evidence.rzp_records = [{"id": "rzp-1", "gross_amount": 100, "order_receipt": "refA"}]

    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "EXCEPTION"
    assert decision.is_valid

def test_incomplete_1_n_rejected():
    proposal = ProposedMatchSchema(
        decision="PROPOSE_MATCH",
        match_type="ONE_TO_MANY",
        evidence_ids=["erp-1", "rzp-1"], # AI left out rzp-2
        confidence=0.99,
        reasoning="test"
    )
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 200, "reference_id": "refA"}]
    evidence.rzp_records = [
        {"id": "rzp-1", "gross_amount": 100, "order_receipt": "refA", "rzp_settlement_id": "set1"},
        {"id": "rzp-2", "gross_amount": 100, "order_receipt": "refB", "rzp_settlement_id": "set1"}
    ]

    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "REJECTED"
    assert "Incomplete 1:N group" in decision.reason

def test_fee_discrepancy_rejected_if_amounts_dont_align():
    proposal = ProposedMatchSchema(
        decision="PROPOSE_MATCH",
        match_type="FEE_DISCREPANCY",
        evidence_ids=["erp-1", "rzp-1"],
        confidence=0.99,
        reasoning="test"
    )
    evidence = EvidencePackage(candidate={})
    # ERP expects 100. RZP says gross 100, fee 5, tax 0 -> net is 95.
    # But proposal didn't provide a discrepancy field, so it's rejected.
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "refA"}]
    evidence.rzp_records = [{"id": "rzp-1", "gross_amount": 100, "fee": 5, "tax": 0, "order_receipt": "refA"}]

    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "REJECTED"
    assert "Fee discrepancy not correctly accounted for" in decision.reason

def test_partial_match_rejected():
    proposal = ProposedMatchSchema(
        decision="PROPOSE_MATCH",
        match_type="PARTIAL",
        evidence_ids=["erp-1", "rzp-1"],
        confidence=0.99,
        reasoning="test"
    )
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "refA"}]
    evidence.rzp_records = [{"id": "rzp-1", "gross_amount": 100, "fee": 5, "tax": 0, "order_receipt": "refA"}]

    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "REJECTED"
    assert "PARTIAL matches cannot be auto-resolved currently" in decision.reason

def test_adversarial_same_amount_unrelated_ref_and_date():
    proposal = ProposedMatchSchema(
        decision="PROPOSE_MATCH",
        match_type="ONE_TO_ONE",
        evidence_ids=["erp-1", "rzp-1"],
        confidence=0.99,
        reasoning="looks good"
    )
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "apple", "timestamp": "2023-01-01"}]
    evidence.rzp_records = [{"id": "rzp-1", "gross_amount": 100, "order_receipt": "orange", "timestamp": "2023-12-31"}]

    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "REJECTED"
    assert "Neither amount nor reference match" not in decision.reason # it hits amount match but ref mismatch
    assert "Amount-only match is forbidden" in decision.reason

def test_adversarial_high_conf_insufficient_evidence():
    proposal = ProposedMatchSchema(
        decision="PROPOSE_MATCH",
        match_type="ONE_TO_ONE",
        evidence_ids=["erp-1"], # missing RZP
        confidence=0.99,
        reasoning="trust me"
    )
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "apple", "timestamp": "2023-01-01"}]

    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "REJECTED"
    assert "ONE_TO_ONE requires exactly 1 ERP and 1 RZP" in decision.reason
