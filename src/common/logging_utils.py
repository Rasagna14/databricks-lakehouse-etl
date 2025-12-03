import logging
from typing import Optional


def get_logger(name: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger(name if name else __name__)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger
