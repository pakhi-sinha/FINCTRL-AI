from typing import Dict, Any, List
from finctrl.backend.engine.ai.schemas import ProposedMatchSchema
from finctrl.backend.engine.ai.evidence import EvidencePackage

class PolicyDecision:
    def __init__(self, action: str, reason: str, is_valid: bool = True):
        self.action = action # AUTO_RESOLVE, HUMAN_REVIEW_REQUIRED, EXCEPTION, REJECTED
        self.reason = reason
        self.is_valid = is_valid

def evaluate_policy(proposal: ProposedMatchSchema, evidence: EvidencePackage) -> PolicyDecision:
    evidence_ids = proposal.supporting_evidence if proposal.supporting_evidence else []
    if not evidence_ids and hasattr(proposal, "evidence_ids") and proposal.evidence_ids:
        evidence_ids = proposal.evidence_ids

    # 1. Validation of Evidence IDs
    supplied_ids = set()
    for erp in evidence.erp_records:
        supplied_ids.add(erp["id"])
    for rzp in evidence.rzp_records:
        supplied_ids.add(rzp["id"])
    for b in evidence.bank_records:
        supplied_ids.add(b["id"])

    for eid in evidence_ids:
        if eid not in supplied_ids:
            return PolicyDecision("REJECTED", f"Hallucinated evidence ID: {eid}", False)

    # 2. Check Decision Type
    if proposal.classification == "EXCEPTION" or getattr(proposal, "decision", "") in ["PROPOSE_EXCEPTION", "NO_MATCH"]:
        return PolicyDecision("EXCEPTION", "AI proposed an exception or NO_MATCH", True)
    elif proposal.classification == "UNRESOLVED":
        return PolicyDecision("HUMAN_REVIEW_REQUIRED", "AI explicitly marked as UNRESOLVED", True)

    # 3. Confidence Thresholds
    if proposal.confidence < 0.75:
        return PolicyDecision("EXCEPTION", f"Confidence {proposal.confidence} < 0.75", True)

    proposed_erp = [r for r in evidence.erp_records if r["id"] in evidence_ids]
    proposed_rzp = [r for r in evidence.rzp_records if r["id"] in evidence_ids]
    proposed_bank = [r for r in evidence.bank_records if r["id"] in evidence_ids]

    # Map classification to logic
    match_type = getattr(proposal, "match_type", None)

    # Financial Safety Rules:
    if match_type == "ONE_TO_ONE":
        if len(proposed_erp) != 1 or len(proposed_rzp) != 1:
            return PolicyDecision("REJECTED", "ONE_TO_ONE requires exactly 1 ERP and 1 RZP", False)

        erp = proposed_erp[0]
        rzp = proposed_rzp[0]

        # Handle Phase 3 vs Phase 4 dictionary keys seamlessly
        rzp_amount = rzp.get("amount", rzp.get("gross_amount", 0))
        rzp_ref = rzp.get("rzp_order_id", rzp.get("order_receipt", ""))

        amount_match = (erp.get("amount", 0) == rzp_amount)
        ref_match = (erp.get("reference_id", "") == rzp_ref)

        if not amount_match and not ref_match:
             return PolicyDecision("REJECTED", "Neither amount nor reference match.", False)

        if amount_match and not ref_match:
            return PolicyDecision("REJECTED", "Amount-only match is forbidden.", False)

        if ref_match and not amount_match:
             return PolicyDecision("REJECTED", "Reference-only match is forbidden.", False)

    elif match_type == "ONE_TO_MANY":
        if len(proposed_erp) < 1 or len(proposed_rzp) < 1:
             return PolicyDecision("REJECTED", "ONE_TO_MANY requires multiple records.", False)

        if len(proposed_rzp) > 0:
            settlement_id = proposed_rzp[0].get("rzp_settlement_id")
            if settlement_id:
                evidence_rzp_with_settlement = [r for r in evidence.rzp_records if r.get("rzp_settlement_id") == settlement_id]
                if len(evidence_rzp_with_settlement) != len(proposed_rzp):
                    return PolicyDecision("REJECTED", "Incomplete 1:N group proposed.", False)

    elif match_type == "FEE_DISCREPANCY":
        if len(proposed_erp) != 1 or len(proposed_rzp) != 1:
            return PolicyDecision("REJECTED", "FEE_DISCREPANCY requires exactly 1 ERP and 1 RZP", False)

        erp = proposed_erp[0]
        rzp = proposed_rzp[0]

        rzp_ref = rzp.get("rzp_order_id", rzp.get("order_receipt", ""))
        if erp.get("reference_id") != rzp_ref:
            return PolicyDecision("REJECTED", "Reference must match for FEE_DISCREPANCY", False)

        expected_net = erp.get("amount", 0)
        rzp_gross = rzp.get("amount", rzp.get("gross_amount", 0))
        calculated_net = rzp_gross - rzp.get("fee", 0) - rzp.get("tax", 0)

        has_discrepancy = getattr(proposal, "discrepancy", None) is not None
        if calculated_net != expected_net and not has_discrepancy:
            return PolicyDecision("REJECTED", "Fee discrepancy not correctly accounted for.", False)

    elif match_type == "PARTIAL":
        return PolicyDecision("REJECTED", "PARTIAL matches cannot be auto-resolved currently.", False)

    # Phase 4 strict requires_human_approval override
    if proposal.requires_human_approval:
        return PolicyDecision("HUMAN_REVIEW_REQUIRED", "AI explicitly requested human approval.", True)

    if proposal.confidence >= 0.95:
        # Phase 4 recommendation respect
        if proposal.recommended_action == "HUMAN_REVIEW_REQUIRED":
            return PolicyDecision("HUMAN_REVIEW_REQUIRED", "High confidence but human review recommended.", True)
        return PolicyDecision("AUTO_RESOLVE", "Confidence >= 0.95 and safety checks passed.", True)
    else:
        return PolicyDecision("HUMAN_REVIEW_REQUIRED", f"Confidence {proposal.confidence} requires review.", True)
