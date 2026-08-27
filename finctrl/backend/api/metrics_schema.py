from pydantic import BaseModel

class MetricsResponse(BaseModel):
    records_processed: int
    records_reconciled: int
    exceptions_created: int
    exceptions_resolved: int
    exceptions_escalated: int
    candidates_created: int

    # Timing/Latency metrics mapped dynamically (can just be simple counters for now based on FinancialEvents if latency isn't persisted)
    processing_failures: int
    average_investigation_latency: float
