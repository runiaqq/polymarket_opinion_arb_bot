"""
Structured logging helper for arb_core.
"""

import logging
import sys
from datetime import datetime
from typing import Any


class StructuredFormatter(logging.Formatter):
    """Formatter that outputs structured log messages."""

    def format(self, record: logging.LogRecord) -> str:
        # Base message
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        level = record.levelname
        name = record.name
        message = record.getMessage()

        # Build structured output
        base = f"{timestamp} | {level:8s} | {name} | {message}"

        # Add extra fields if present
        extras = []
        for key, value in record.__dict__.items():
            if key not in (
                "name",
                "msg",
                "args",
                "created",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "exc_info",
                "exc_text",
                "thread",
                "threadName",
                "message",
                "taskName",
            ):
                extras.append(f"{key}={value}")

        if extras:
            base += " | " + " ".join(extras)

        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)

        return base


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Get a structured logger.

    Args:
        name: Logger name (typically __name__)
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(StructuredFormatter())
    logger.addHandler(handler)

    # Don't propagate to root logger
    logger.propagate = False

    return logger


def log_with_context(
    logger: logging.Logger, level: str, message: str, **context: Any
) -> None:
    """
    Log a message with additional context fields.

    Args:
        logger: Logger instance
        level: Log level
        message: Log message
        **context: Additional context fields to include
    """
    extra = context
    log_method = getattr(logger, level.lower(), logger.info)
    log_method(message, extra=extra)
