import os
import bz2
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Iterator, Dict, Any, Set, List, Tuple
from collections import defaultdict
from dataclasses import dataclass

# =========================
# Models
# =========================
class WikiPage:
    def __init__(self, page_id: int=0, title: str=""):
        self.page_id: int = page_id
        self.title: str = title
        self.revisions: List[Dict[str, Any]] = []  # List of revision dicts sorted by timestamp

    def append_revision(self, revision: Dict[str, Any]) -> None:
        """Append a revision to the page's revision list"""
        self.revisions.append(revision)

    def sort_revision_by_timestamp(self, ascending: bool = True) -> None:
        """Sort revisions by timestamp in ascending order"""
        self.revisions.sort(key=lambda r: r.get("timestamp", ""), reverse=not ascending)
    
    def is_empty(self) -> bool:
        """Check if the page has no revisions"""
        return len(self.revisions) == 0

# =========================
# Logging Setup
# =========================

def setup_logging(log_dir: Path = None, log_level: str = "INFO") -> logging.Logger:
    """
    Setup logging configuration.
    
    Args:
        log_dir: Directory to save log files. If None, only console logging.
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    
    Returns:
        Configured logger
    """
    logger = logging.getLogger("filtering")
    logger.setLevel(getattr(logging, log_level.upper()))
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S"
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler (if log_dir provided)
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"filtering_{timestamp}.log"
        
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(funcName)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)
        
        logger.info(f"Log file: {log_file}")
    
    return logger


logger = logging.getLogger("filtering")


# =========================
# IO Utilities
# =========================

def read_jsonl_bz2(path: Path) -> Iterator[Dict[str, Any]]:
    """Stream read .jsonl.bz2 file"""
    with bz2.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


# =========================
# Validation Logic
# =========================

def is_bot_revision(
    revision: Dict[str, Any],
) -> bool:
    """Check if revision was created by a bot"""
    # Check is_bot field
    return revision.get("is_bot", False)


# =========================
# Processing
# =========================

def filter_revisions(
    input_dir: Path,
    output_dir: Path=None,
) -> Tuple[Dict[int, List[Dict[str, Any]]], int, int]:
    """
    Iterate over all .jsonl.bz2 files in directory
    and filter out bot revisions and consecutive same-user revisions.
    
    Returns:
        Tuple of (articles_dict, bot_count, consecutive_count)
        where articles_dict maps page_id to list of revisions sorted by timestamp
    """
    
    files = sorted(input_dir.glob("*.jsonl.bz2"))
    os.makedirs(output_dir, exist_ok=True)
    
    # Group revisions by page_id to detect consecutive same-user revisions
    # First pass: read all revisions and filter bots
    for file_path in files:
        logger.info(f"Processing: {file_path}")
        output_path = output_dir / file_path.name if output_dir else None
        
        current_page_id = None
        current_page_title = None
        current_page = WikiPage()
        current_username = None
        latest_rev_of_user = None
        bot_revision_count = 0
        total_revision_count = 0

        for revision in read_jsonl_bz2(file_path):
            page_id = revision.pop("page_id")
            page_title = revision.pop("page_title", None)
            
            # khi chuyển sang page mới
            if page_id != current_page_id or page_title != current_page_title:
                if latest_rev_of_user is not None: # commit revision cuối cùng của user cũ trước khi chuyển page
                    current_page.append_revision(latest_rev_of_user)
                    latest_rev_of_user = None
                    current_username = None

                if current_page_id is not None and not current_page.is_empty(): # chỉ lưu lại nếu tập revisions của page không rỗng
                    # sort revisions by timestamp before writing
                    current_page.sort_revision_by_timestamp(ascending=True)
                    with bz2.open(output_path, "at", encoding="utf-8") as f:
                        f.write(json.dumps(current_page.__dict__, ensure_ascii=False) + "\n")

                logger.info(f"Page {current_page_id} - {current_page_title} | Total revisions: {total_revision_count} | Total bot revisions: {bot_revision_count}")

                current_page_id = page_id
                current_page_title = page_title
                current_page = WikiPage(page_id=page_id, title=page_title)
                total_revision_count = 0
                bot_revision_count = 0

            # tiếp tục page hiện tại
            total_revision_count += 1
            
            # nếu là revision của bot thì bỏ qua và đếm số lượng bot revision đã bỏ qua
            if is_bot_revision(revision):
                bot_revision_count += 1
                continue

            username = revision.get("username", None)
            # khi chuyển username mới, commit revision cuối cùng của user cũ (nếu có) và reset latest_rev_of_user
            if username != current_username:
                if latest_rev_of_user is not None:
                    current_page.append_revision(latest_rev_of_user)
                current_username = username

            # luôn cập nhật revision cuối của run
            latest_rev_of_user = revision
        # end of for loops

        # ==== FLUSH RUN CUỐI CÙNG ====
        if latest_rev_of_user is not None:
            current_page.append_revision(latest_rev_of_user)

        # ==== FLUSH PAGE CUỐI CÙNG ====
        if current_page_id is not None and not current_page.is_empty():
            # nếu bạn vẫn muốn sort thì giữ, còn không có thể bỏ
            current_page.sort_revision_by_timestamp(ascending=True)

            with bz2.open(output_path, "at", encoding="utf-8") as f:
                f.write(json.dumps(current_page.__dict__, ensure_ascii=False) + "\n")


# =========================
# CLI
# =========================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Filter invalid Wikipedia revisions from .jsonl.bz2 files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--input_dir",
        type=Path,
        required=True,
        help="Directory containing .jsonl.bz2 revision files",
    )

    parser.add_argument(
        "--output_dir",
        type=Path,
        required=True,
        help="Output directory for filtered .jsonl.bz2 files",
    )

    parser.add_argument(
        "--log_dir",
        type=Path,
        default=None,
        help="Directory to save log files (optional)",
    )

    parser.add_argument(
        "--log_level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    
    # Setup logging
    setup_logging(log_dir=args.log_dir, log_level=args.log_level)
    
    logger.info("Starting filtering process")
    logger.info(f"Input directory: {args.input_dir}")
    logger.info(f"Output directory: {args.output_dir}")

    filter_revisions(input_dir=args.input_dir, output_dir=args.output_dir)

    logger.info("=" * 50)
    logger.info("Filtering completed.")


if __name__ == "__main__":
    main()