import json
from uuid import UUID
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from finctrl.backend.engine.ai.provider import get_ai_provider, AIProvider
from finctrl.backend.engine.ai.evidence import collect_evidence, EvidencePackage
from finctrl.backend.engine.ai.tools import TOOLS_SCHEMA, execute_tool
from finctrl.backend.engine.policy import evaluate_policy
from finctrl.backend.engine.action import apply_action, _create_audit
from finctrl.backend.engine.ai.schemas import ProposedMatchSchema

class AIAgent:
    def __init__(self, db: AsyncSession, provider: Optional[AIProvider] = None):
        self.db = db
        self.provider = provider or get_ai_provider()

    async def investigate_candidate(self, candidate_id: str):
        _create_audit(self.db, "CANDIDATE", UUID(candidate_id), "AI_INVESTIGATION_STARTED", {})
        await self.db.commit()

        # 1. Collect bounded evidence
        evidence = await collect_evidence(self.db, candidate_id)
        if not evidence:
            _create_audit(self.db, "CANDIDATE", UUID(candidate_id), "AI_INVESTIGATION_FAILED", {"reason": "Candidate not found"})
            await self.db.commit()
            return

        # 2. Prepare AI Prompt
        prompt = (
            f"Investigate the following reconciliation candidate.\n"
            f"Candidate Data: {json.dumps(evidence.candidate, indent=2)}\n\n"
            f"ERP Records: {json.dumps(evidence.erp_records, indent=2)}\n\n"
            f"Razorpay Records: {json.dumps(evidence.rzp_records, indent=2)}\n\n"
            f"Bank Records: {json.dumps(evidence.bank_records, indent=2)}\n\n"
            f"You may use tools to search for additional evidence if needed.\n"
            f"Once you have sufficient evidence, provide a final ProposedMatchSchema.\n"
        )

        messages = [
            {"role": "system", "content": "You are a financial reconciliation AI. You strictly return JSON matching ProposedMatchSchema."},
            {"role": "user", "content": prompt}
        ]

        max_turns = 5
        turn = 0
        proposal = None

        # Tool call loop
        while turn < max_turns:
            try:
                response_msg = await self.provider.chat(messages=messages, tools=TOOLS_SCHEMA)
            except Exception as e:
                _create_audit(self.db, "CANDIDATE", UUID(candidate_id), "AI_INVESTIGATION_FAILED", {"reason": str(e)})
                await self.db.commit()
                return

            messages.append(response_msg.model_dump(exclude_none=True))

            if response_msg.tool_calls:
                for tool_call in response_msg.tool_calls:
                    fn_name = tool_call.function.name
                    try:
                        args = json.loads(tool_call.function.arguments)
                    except json.JSONDecodeError:
                        args = {}

                    result = await execute_tool(self.db, fn_name, args)

                    if "fetch_razorpay" in fn_name:
                        _create_audit(self.db, "CANDIDATE", UUID(candidate_id), "RAZORPAY_EVIDENCE_FETCHED", {"tool": fn_name, "args": args})
                        await self.db.commit()

                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "name": fn_name,
                        "content": json.dumps(result)
                    })
            else:
                # Expecting structured JSON
                try:
                    data = json.loads(response_msg.content)
                    proposal = ProposedMatchSchema(**data)
                except Exception as e:
                    _create_audit(self.db, "CANDIDATE", UUID(candidate_id), "AI_INVESTIGATION_FAILED", {"reason": f"Failed to parse ProposedMatchSchema: {e}"})
                    await self.db.commit()
                    return
                break

            turn += 1

        if not proposal:
            _create_audit(self.db, "CANDIDATE", UUID(candidate_id), "AI_INVESTIGATION_FAILED", {"reason": "Max turns reached without proposal"})
            await self.db.commit()
            return

        _create_audit(self.db, "CANDIDATE", UUID(candidate_id), "AI_PROPOSED", proposal.model_dump())
        await self.db.commit()

        # 3. Policy Layer
        decision = evaluate_policy(proposal, evidence)

        # 4. Action Layer
        await apply_action(self.db, candidate_id, decision, proposal, evidence)
