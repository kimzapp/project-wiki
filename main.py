# =====================================================
#                       MAIN
# =====================================================

import argparse
from utils.logging import setup_logging
from utils.bz2_stream import open_bz2_stream
from app.revision_processor import WikipediaRevisionProcessor
from pathlib import Path
from typing import List


def list_bz2_files(input_dir: str) -> List[Path]:
    p = Path(input_dir)

    if not p.exists():
        raise FileNotFoundError(f"Input path not found: {input_dir}")
    if not p.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {input_dir}")

    return sorted(
        [f for f in p.iterdir() if f.is_file() and f.suffix == ".bz2"],
        key=lambda x: x.name,
    )

def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Wikipedia revision extraction pipeline.\n\n"
            "This script processes one or more Wikipedia XML.bz2 dump files, "
            "extracts revision history for main-namespace pages, cleans wiki markup, "
            "and writes results to a JSONL file."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # --------------------------------------------------
    # Input / Output
    # --------------------------------------------------
    parser.add_argument(
        "input",
        help=(
            "Path to a directory containing Wikipedia XML.bz2 dump files. "
            "All .bz2 files in the directory will be processed in "
            "lexicographical order."
        ),
    )

    parser.add_argument(
        "-o", "--output",
        default="wiki_revisions_clean.jsonl",
        help=(
            "Path to output JSONL file. "
            "Results from all processed dump files will be appended to this file."
        ),
    )

    # --------------------------------------------------
    # Processing control
    # --------------------------------------------------
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help=(
            "Maximum number of Wikipedia pages to process across ALL input files. "
            "If not set, all pages will be processed."
        ),
    )

    parser.add_argument(
        "--log-every",
        type=int,
        default=100_000,
        help=(
            "Log progress every N processed revisions. "
            "Use smaller values for more frequent logging during debugging."
        ),
    )

    # --------------------------------------------------
    # Logging
    # --------------------------------------------------
    parser.add_argument(
        "--log-dir",
        default="logs",
        help=(
            "Directory where log files will be written. "
            "A new log file with a timestamped name will be created for each run."
        ),
    )

    return parser.parse_args()


def main():
    args = parse_args()
    logger = setup_logging(log_dir=args.log_dir)
    bz2_files = list_bz2_files(args.input)
    logger.info("Found %d bz2 files", len(bz2_files))

    processor = WikipediaRevisionProcessor(
        output_path=args.output,
        log_every_n=args.log_every,
        max_pages=args.max_pages,
        logger=logger,
    )

    for idx, bz2_path in enumerate(bz2_files, 1):
        if processor.finished:
            break

        logger.info(
            "[%d/%d] Processing file: %s",
            idx,
            len(bz2_files),
            bz2_path.name,
        )

        with open_bz2_stream(bz2_path) as stream:
            processor.process(stream)

    logger.info(
        "DONE | pages=%d | revisions=%d",
        processor.page_count,
        processor.revision_count,
    )


if __name__ == "__main__":
    main()