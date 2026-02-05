# =====================================================
#           INFRASTRUCTURE / LOGGING
# =====================================================

import logging
import os
from pathlib import Path
from datetime import datetime


def setup_logging(
    log_dir: str = "logs",
    log_prefix: str = "run",
) -> logging.Logger:
    """
    Setup logging với file và console output.
    
    Args:
        log_dir: Directory để lưu log files.
        log_prefix: Prefix cho tên file log (default: "run").
    
    Returns:
        Logger instance.
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    pid = os.getpid()
    log_file = Path(log_dir) / f"{log_prefix}_{timestamp}_{pid}.log"

    # Tạo logger riêng cho mỗi process để tránh conflict
    logger_name = f"wiki-pipeline-{pid}"
    logger = logging.getLogger(logger_name)
    
    # Tránh duplicate handlers nếu logger đã tồn tại
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # File handler
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info("Logging initialized")
    logger.info(f"Log file: {log_file}")

    return logger
