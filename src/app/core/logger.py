import logging
import os
import sys
from logging.handlers import RotatingFileHandler

from .log_context import ExecutionLogContextFilter

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
LOG_FILE_PATH = os.path.join(LOG_DIR, "app.log")

LOGGING_FORMAT = (
    "%(asctime)s - %(name)s - %(levelname)s - "
    "run_id=%(run_id)s arq_job_id=%(arq_job_id)s job_try=%(job_try)s - %(message)s"
)

_VERBOSITY_TO_LEVEL = {
    "full": logging.DEBUG,
    "medium": logging.INFO,
    "minimal": logging.WARNING,
}


def _get_logging_level() -> int:
    try:
        from .config import settings

        verbosity = getattr(settings, "LOG_VERBOSITY", "medium")
        return _VERBOSITY_TO_LEVEL.get(verbosity, logging.INFO)
    except Exception:
        return _VERBOSITY_TO_LEVEL.get(
            os.getenv("LOG_VERBOSITY", "medium"), logging.INFO
        )


LOGGING_LEVEL = _get_logging_level()

root_logger = logging.getLogger("")
if not root_logger.handlers:
    logging.basicConfig(
        level=LOGGING_LEVEL,
        format=LOGGING_FORMAT,
        stream=sys.stdout,
    )
# going to use dozzle or loki or something to ingest the logs properly

_logger = logging.getLogger(__name__)

try:
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR, exist_ok=True)

    file_handler = RotatingFileHandler(LOG_FILE_PATH, maxBytes=10485760, backupCount=5)
    file_handler.setLevel(LOGGING_LEVEL)
    file_handler.setFormatter(logging.Formatter(LOGGING_FORMAT))
    root_logger.addHandler(file_handler)
except (PermissionError, OSError) as e:
    _logger.warning("event=logger_file_handler_failed error=%s", str(e))


has_stream_handler = any(
    isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout
    for handler in root_logger.handlers
)

if not has_stream_handler:
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(LOGGING_LEVEL)
    stream_handler.setFormatter(logging.Formatter(LOGGING_FORMAT))
    root_logger.addHandler(stream_handler)

_exec_ctx_filter = ExecutionLogContextFilter()
for _handler in root_logger.handlers:
    _handler.addFilter(_exec_ctx_filter)
    _handler.setFormatter(logging.Formatter(LOGGING_FORMAT))
