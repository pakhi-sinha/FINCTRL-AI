import os
os.environ['DATABASE_URL'] = 'sqlite+aiosqlite:///:memory:'

import pytest
from finctrl.backend.engine.ai.schemas import ProposedMatchSchema
from finctrl.backend.engine.ai.evidence import EvidencePackage
from finctrl.backend.engine.policy import evaluate_policy

def test_hallucinated_evidence_id():
    proposal = ProposedMatchSchema(classification="MATCH", recommended_action="AUTO_RESOLVE", risk_level="LOW", supporting_evidence=["erp-1", "rzp-1"], confidence=0.99, reason="test")
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1"}]
    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "REJECTED"
    assert not decision.is_valid

def test_amount_only_match_rejected():
    proposal = ProposedMatchSchema(classification="MATCH", match_type="ONE_TO_ONE", recommended_action="AUTO_RESOLVE", risk_level="LOW", supporting_evidence=["erp-1", "rzp-1"], confidence=0.99, reason="test")
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "refA"}]
    evidence.rzp_records = [{"id": "rzp-1", "amount": 100, "rzp_order_id": "refB"}]
    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "REJECTED"
    assert "Amount-only match is forbidden" in decision.reason
    assert not decision.is_valid

def test_auto_resolve_valid():
    proposal = ProposedMatchSchema(classification="MATCH", match_type="ONE_TO_ONE", recommended_action="AUTO_RESOLVE", risk_level="LOW", supporting_evidence=["erp-1", "rzp-1"], confidence=0.96, reason="test")
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "refA"}]
    evidence.rzp_records = [{"id": "rzp-1", "amount": 100, "rzp_order_id": "refA"}]
    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "AUTO_RESOLVE"
    assert decision.is_valid

def test_human_review_required():
    proposal = ProposedMatchSchema(classification="MATCH", match_type="ONE_TO_ONE", recommended_action="HUMAN_REVIEW_REQUIRED", risk_level="LOW", supporting_evidence=["erp-1", "rzp-1"], confidence=0.85, requires_human_approval=True, reason="test")
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "refA"}]
    evidence.rzp_records = [{"id": "rzp-1", "amount": 100, "rzp_order_id": "refA"}]
    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "HUMAN_REVIEW_REQUIRED"
    assert decision.is_valid

def test_exception_low_confidence():
    proposal = ProposedMatchSchema(classification="MATCH", match_type="ONE_TO_ONE", recommended_action="AUTO_RESOLVE", risk_level="LOW", supporting_evidence=["erp-1", "rzp-1"], confidence=0.60, reason="test")
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "refA"}]
    evidence.rzp_records = [{"id": "rzp-1", "amount": 100, "rzp_order_id": "refA"}]
    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "EXCEPTION"
    assert decision.is_valid

def test_incomplete_1_n_rejected():
    proposal = ProposedMatchSchema(classification="MATCH", match_type="ONE_TO_MANY", recommended_action="AUTO_RESOLVE", risk_level="LOW", supporting_evidence=["erp-1", "rzp-1"], confidence=0.99, reason="test")
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 200, "reference_id": "refA"}]
    evidence.rzp_records = [{"id": "rzp-1", "amount": 100, "rzp_order_id": "refA", "rzp_settlement_id": "set1"}, {"id": "rzp-2", "amount": 100, "rzp_order_id": "refB", "rzp_settlement_id": "set1"}]
    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "REJECTED"
    assert "Incomplete 1:N group" in decision.reason
    assert not decision.is_valid

def test_fee_discrepancy_rejected_if_amounts_dont_align():
    proposal = ProposedMatchSchema(classification="MATCH", match_type="FEE_DISCREPANCY", recommended_action="AUTO_RESOLVE", risk_level="LOW", supporting_evidence=["erp-1", "rzp-1"], confidence=0.99, reason="test")
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "refA"}]
    evidence.rzp_records = [{"id": "rzp-1", "amount": 100, "fee": 5, "tax": 0, "rzp_order_id": "refA"}]
    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "REJECTED"
    assert "Fee discrepancy not correctly accounted for" in decision.reason
    assert not decision.is_valid

def test_partial_match_rejected():
    proposal = ProposedMatchSchema(classification="MATCH", match_type="PARTIAL", recommended_action="AUTO_RESOLVE", risk_level="LOW", supporting_evidence=["erp-1", "rzp-1"], confidence=0.99, reason="test")
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "refA"}]
    evidence.rzp_records = [{"id": "rzp-1", "amount": 100, "fee": 5, "tax": 0, "rzp_order_id": "refA"}]
    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "REJECTED"
    assert "PARTIAL matches cannot be auto-resolved currently" in decision.reason
    assert not decision.is_valid

def test_adversarial_same_amount_unrelated_ref_and_date():
    proposal = ProposedMatchSchema(classification="MATCH", match_type="ONE_TO_ONE", recommended_action="AUTO_RESOLVE", risk_level="LOW", supporting_evidence=["erp-1", "rzp-1"], confidence=0.99, reason="looks good")
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "apple", "timestamp": "2023-01-01"}]
    evidence.rzp_records = [{"id": "rzp-1", "amount": 100, "rzp_order_id": "orange", "timestamp": "2023-12-31"}]
    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "REJECTED"
    assert "Neither amount nor reference match" not in decision.reason
    assert "Amount-only match is forbidden" in decision.reason
    assert not decision.is_valid

def test_adversarial_high_conf_insufficient_evidence():
    proposal = ProposedMatchSchema(classification="MATCH", match_type="ONE_TO_ONE", recommended_action="AUTO_RESOLVE", risk_level="LOW", supporting_evidence=["erp-1"], confidence=0.99, reason="trust me")
    evidence = EvidencePackage(candidate={})
    evidence.erp_records = [{"id": "erp-1", "amount": 100, "reference_id": "apple", "timestamp": "2023-01-01"}]
    decision = evaluate_policy(proposal, evidence)
    assert decision.action == "REJECTED"
    assert "ONE_TO_ONE requires exactly 1 ERP and 1 RZP" in decision.reason
    assert not decision.is_valid
