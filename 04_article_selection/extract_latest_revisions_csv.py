#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bz2
import csv
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parent.parent

    parser = argparse.ArgumentParser(
        description=(
            "Extract latest revision per article from histories_filtered_cite "
            "and export a MySQL-friendly CSV."
        )
    )
    parser.add_argument(
        "--input_dir",
        type=Path,
        default=repo_root / "histories_filtered_cite",
        help="Directory containing .jsonl.bz2 files (default: histories_filtered_cite).",
    )
    parser.add_argument(
        "--output_csv",
        type=Path,
        default=repo_root / "ranking_results_cite" / "latest_revisions_for_mysql.csv",
        help=(
            "Output CSV path (default: "
            "ranking_results_cite/latest_revisions_for_mysql.csv)."
        ),
    )
    return parser.parse_args()


def latest_revision(revisions: list[dict]) -> dict | None:
    if not revisions:
        return None
    return max(revisions, key=lambda rev: (str(rev.get("timestamp", "")), int(rev.get("revision_id", -1))))


def iter_jsonl_bz2(path: Path):
    with bz2.open(path, "rt", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                # Skip malformed lines to keep long runs resilient.
                continue


def main() -> None:
    args = parse_args()
    input_dir = args.input_dir
    output_csv = args.output_csv

    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    files = sorted(input_dir.glob("*.jsonl.bz2"))
    if not files:
        raise FileNotFoundError(f"No .jsonl.bz2 files found in: {input_dir}")

    output_csv.parent.mkdir(parents=True, exist_ok=True)

    file_count = 0
    article_count = 0
    skipped_count = 0

    with output_csv.open("w", encoding="utf-8", newline="") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(["id", "title", "raw_text", "citation_count"])

        for file_path in files:
            file_count += 1
            for record in iter_jsonl_bz2(file_path):
                page_id = record.get("page_id")
                title = record.get("title", "")
                revisions = record.get("revisions", [])
                rev = latest_revision(revisions)

                if page_id is None or rev is None:
                    skipped_count += 1
                    continue

                raw_text = rev.get("raw_text", "")
                citation_count = rev.get("citation_count", 0)

                writer.writerow([page_id, title, raw_text, citation_count])
                article_count += 1

    print(f"Input files processed: {file_count}")
    print(f"Articles exported: {article_count}")
    print(f"Records skipped: {skipped_count}")
    print(f"Output CSV: {output_csv}")


if __name__ == "__main__":
    main()
