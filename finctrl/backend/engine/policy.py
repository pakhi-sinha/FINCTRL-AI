from typing import Dict, Any, List
from finctrl.backend.engine.ai.schemas import ProposedMatchSchema
from finctrl.backend.engine.ai.evidence import EvidencePackage

class PolicyDecision:
    def __init__(self, action: str, reason: str, is_valid: bool = True):
        self.action = action # AUTO_RESOLVE, HUMAN_REVIEW_REQUIRED, EXCEPTION, REJECTED
        self.reason = reason
        self.is_valid = is_valid

def evaluate_policy(proposal: ProposedMatchSchema, evidence: EvidencePackage) -> PolicyDecision:
    # 1. Validation of Evidence IDs
    supplied_ids = set()
    for erp in evidence.erp_records:
        supplied_ids.add(erp["id"])
    for rzp in evidence.rzp_records:
        supplied_ids.add(rzp["id"])
    for b in evidence.bank_records:
        supplied_ids.add(b["id"])

    for eid in proposal.evidence_ids:
        if eid not in supplied_ids:
            return PolicyDecision("REJECTED", f"Hallucinated evidence ID: {eid}", False)

    # 2. Check Decision Type
    if proposal.decision == "NO_MATCH":
        return PolicyDecision("EXCEPTION", "AI determined NO_MATCH", True)
    elif proposal.decision == "PROPOSE_EXCEPTION":
        return PolicyDecision("EXCEPTION", "AI proposed an exception: " + str(proposal.reasoning), True)

    # 3. Confidence Thresholds
    if proposal.confidence < 0.75:
        return PolicyDecision("EXCEPTION", f"Confidence {proposal.confidence} < 0.75", True)

    proposed_erp = [r for r in evidence.erp_records if r["id"] in proposal.evidence_ids]
    proposed_rzp = [r for r in evidence.rzp_records if r["id"] in proposal.evidence_ids]
    proposed_bank = [r for r in evidence.bank_records if r["id"] in proposal.evidence_ids]

    # Financial Safety Rules:
    if proposal.match_type == "ONE_TO_ONE":
        if len(proposed_erp) != 1 or len(proposed_rzp) != 1:
            return PolicyDecision("REJECTED", "ONE_TO_ONE requires exactly 1 ERP and 1 RZP", False)

        erp = proposed_erp[0]
        rzp = proposed_rzp[0]

        amount_match = (erp["amount"] == rzp["gross_amount"])
        ref_match = (erp["reference_id"] == rzp["order_receipt"])

        if not amount_match and not ref_match:
             return PolicyDecision("REJECTED", "Neither amount nor reference match.", False)

        if amount_match and not ref_match:
            # Need another signal. Without full date parsing, reject for now.
            return PolicyDecision("REJECTED", "Amount-only match is forbidden.", False)

        if ref_match and not amount_match:
             return PolicyDecision("REJECTED", "Reference-only match is forbidden.", False)

    elif proposal.match_type == "ONE_TO_MANY":
        if len(proposed_erp) < 1 or len(proposed_rzp) < 1:
             return PolicyDecision("REJECTED", "ONE_TO_MANY requires multiple records.", False)

        if len(proposed_rzp) > 0:
            settlement_id = proposed_rzp[0].get("rzp_settlement_id")
            if settlement_id:
                evidence_rzp_with_settlement = [r for r in evidence.rzp_records if r.get("rzp_settlement_id") == settlement_id]
                if len(evidence_rzp_with_settlement) != len(proposed_rzp):
                    return PolicyDecision("REJECTED", "Incomplete 1:N group proposed.", False)

    elif proposal.match_type == "FEE_DISCREPANCY":
        if len(proposed_erp) != 1 or len(proposed_rzp) != 1:
            return PolicyDecision("REJECTED", "FEE_DISCREPANCY requires exactly 1 ERP and 1 RZP", False)

        erp = proposed_erp[0]
        rzp = proposed_rzp[0]

        # Must have reference match
        if erp["reference_id"] != rzp["order_receipt"]:
            return PolicyDecision("REJECTED", "Reference must match for FEE_DISCREPANCY", False)

        expected_net = erp["amount"]
        calculated_net = rzp["gross_amount"] - rzp["fee"] - rzp["tax"]
        if calculated_net != expected_net and not proposal.discrepancy:
            return PolicyDecision("REJECTED", "Fee discrepancy not correctly accounted for.", False)

    elif proposal.match_type == "PARTIAL":
        return PolicyDecision("REJECTED", "PARTIAL matches cannot be auto-resolved currently.", False)
    else:
        return PolicyDecision("REJECTED", "Unknown match type.", False)

    # Check timestamps roughly
    for e in proposed_erp:
        for r in proposed_rzp:
            if "timestamp" in e and "timestamp" in r:
                # Basic check, just verifying it exists for now.
                pass

    if proposal.confidence >= 0.95:
        return PolicyDecision("AUTO_RESOLVE", "Confidence >= 0.95 and safety checks passed.", True)
    else:
        return PolicyDecision("HUMAN_REVIEW_REQUIRED", f"Confidence {proposal.confidence} requires review.", True)
