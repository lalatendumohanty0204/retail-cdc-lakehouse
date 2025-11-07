"""
Logging utilities used across the pipeline.

- Single StreamHandler to stdout (works in terminals, CI, and notebooks)
- Consistent, grep-friendly format with stage name included
- Keeps logging config in one place to avoid duplicated handlers
"""
import logging
import sys

def create_pipeline_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # prevent duplicate handlers when modules re-import
    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger
