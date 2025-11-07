# import logging
#
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

import sys
import os
from loguru import logger

# Remove default handler to prevent duplicate logs
logger.remove()

# Create logs directory if not exists
LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Define log format (shared by all handlers)
LOG_FORMAT = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | "
    "{module}:{function}:{line} | {message}"
)

# Console handler (colorized)
logger.add(
    sys.stderr,
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level:<8}</level> | "
        "<cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    ),
    level="INFO",
    colorize=True,
)

# File handler (plain text)
logger.add(
    os.path.join(LOG_DIR, "app.log"),
    format=LOG_FORMAT,
    rotation="1 day",
    retention="10 days",
    level="DEBUG",
    encoding="utf-8",
)

# Error log (plain text, only errors)
logger.add(
    os.path.join(LOG_DIR, "error.log"),
    format=LOG_FORMAT,
    rotation="1 week",
    retention="10 days",
    level="ERROR",
    encoding="utf-8",
)

# # Exception hook to catch unhandled errors
# def log_exception(exc_type, exc_value, exc_traceback):
#     if issubclass(exc_type, KeyboardInterrupt):
#         sys.__excepthook__(exc_type, exc_value, exc_traceback)
#         return
#     logger.exception("Unhandled exception:", exc_info=(exc_type, exc_value, exc_traceback))

#sys.excepthook = log_exception

__all__ = ["logger"]
