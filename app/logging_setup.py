import json
import logging
import sys
from datetime import datetime, timezone

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        # пробрасываем extra поля
        for k, v in record.__dict__.items():
            if k in ("msg", "args", "levelname", "levelno", "pathname", "filename", "module",
                     "exc_info", "exc_text", "stack_info", "lineno", "funcName", "created",
                     "msecs", "relativeCreated", "thread", "threadName", "processName",
                     "process", "name"):
                continue
            if k.startswith("_"):
                continue
            payload[k] = v
        return json.dumps(payload, ensure_ascii=False)

def setup_logging(level: str) -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())

    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(JsonFormatter())

    root.handlers.clear()
    root.addHandler(h)
