# =====================================================
#           INFRASTRUCTURE / LOGGING
# =====================================================

import logging
from pathlib import Path
from datetime import datetime


def setup_logging(log_dir: str = "logs") -> logging.Logger:
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = Path(log_dir) / f"run_{timestamp}.log"

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),               # console
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )

    logger = logging.getLogger("wiki-pipeline")
    logger.info("Logging initialized")
    logger.info(f"Log file: {log_file}")

    return logger
