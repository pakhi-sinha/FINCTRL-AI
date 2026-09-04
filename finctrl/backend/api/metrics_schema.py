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
    reconciliation_runs_total: int = 0
    reconciliation_runs_succeeded: int = 0
    reconciliation_runs_partial: int = 0
    reconciliation_runs_failed: int = 0
    reconciliation_average_duration_ms: int = 0
    reconciliation_periods_open: int = 0
    reconciliation_periods_ready: int = 0
    reconciliation_periods_blocked: int = 0
    reconciliation_periods_closed: int = 0
    open_exceptions_by_severity: dict[str, int] = {}
