import logging
import json
import traceback
from datetime import datetime, timezone
from contextvars import ContextVar

# Context variable to hold the correlation ID
correlation_id_ctx: ContextVar[str] = ContextVar("correlation_id", default="-")

class JSONLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": correlation_id_ctx.get(),
        }

        if record.exc_info:
            log_record["exception"] = "".join(traceback.format_exception(*record.exc_info))

        if hasattr(record, "extra"):
            log_record.update(record.extra)

        return json.dumps(log_record)

def setup_logging():
    logger = logging.getLogger("finctrl")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if setup_logging is called multiple times
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JSONLogFormatter())
        logger.addHandler(handler)

    return logger

logger = setup_logging()
