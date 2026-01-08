import logging
import os
from logging.handlers import RotatingFileHandler

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
LOG_FILE_PATH = os.path.join(LOG_DIR, "app.log")

LOGGING_LEVEL = logging.INFO
LOGGING_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

logging.basicConfig(level=LOGGING_LEVEL, format=LOGGING_FORMAT)

try:
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR, exist_ok=True)
    
    file_handler = RotatingFileHandler(LOG_FILE_PATH, maxBytes=10485760, backupCount=5)
    file_handler.setLevel(LOGGING_LEVEL)
    file_handler.setFormatter(logging.Formatter(LOGGING_FORMAT))
    logging.getLogger("").addHandler(file_handler)
except (PermissionError, OSError) as e:
    logging.warning(f"Could not set up file logging: {e}. Using console logging only.")
