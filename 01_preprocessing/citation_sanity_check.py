import argparse
import bz2
import csv
import heapq
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, TextIO, Tuple


REF_TAG_PATTERN = re.compile(r"<ref\b[^>]*?/?>", flags=re.IGNORECASE)


@dataclass
class PageStats:
    page_id: int
    page_title: str
    revision_count: int = 0
    total_citations: int = 0
    max_citations_in_revision: int = 0


def count_citations_from_raw(raw_text: str) -> int:
    if not raw_text:
        return 0
    return len(REF_TAG_PATTERN.findall(raw_text))


def iter_input_files(input_path: Path) -> List[Path]:
    if input_path.is_file():
        return [input_path]

    if not input_path.is_dir():
        raise FileNotFoundError(f"Input path not found: {input_path}")

    files = sorted(
        [
            p
            for p in input_path.iterdir()
            if p.is_file() and (p.name.endswith(".jsonl") or p.name.endswith(".jsonl.bz2"))
        ],
        key=lambda p: p.name,
    )

    if not files:
        raise FileNotFoundError(
            f"No .jsonl or .jsonl.bz2 files found in directory: {input_path}"
        )

    return files


def open_text_stream(path: Path) -> TextIO:
    if path.name.endswith(".bz2"):
        return bz2.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def read_records(path: Path) -> Iterator[Dict]:
    with open_text_stream(path) as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at {path}:{line_no}: {exc}") from exc


def maybe_write_page_row(writer: Optional[csv.DictWriter], page: PageStats) -> None:
    if writer is None:
        return
    writer.writerow(
        {
            "page_id": page.page_id,
            "page_title": page.page_title,
            "revision_count": page.revision_count,
            "total_citations": page.total_citations,
            "avg_citations_per_revision": (
                f"{(page.total_citations / page.revision_count):.6f}"
                if page.revision_count > 0
                else "0.000000"
            ),
            "max_citations_in_revision": page.max_citations_in_revision,
        }
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sanity check citation_count by page from preprocessing outputs (.jsonl/.jsonl.bz2)."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "input",
        help="Input file or directory containing preprocessing outputs (.jsonl/.jsonl.bz2)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=20,
        help="Show top K pages with highest total citations",
    )
    parser.add_argument(
        "--per-page-output",
        default=None,
        help="Optional CSV path to write per-page citation stats",
    )
    parser.add_argument(
        "--recount-from-raw",
        action="store_true",
        help=(
            "Recount citations directly from raw_text and report mismatch count with stored citation_count"
        ),
    )
    parser.add_argument(
        "--max-revisions",
        type=int,
        default=None,
        help="Stop early after reading this many revisions (quick sanity run)",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=500_000,
        help="Print progress every N revisions",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_path = Path(args.input)
    files = iter_input_files(input_path)

    page_rows_file = None
    page_writer = None
    if args.per_page_output:
        out_path = Path(args.per_page_output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        page_rows_file = out_path.open("w", encoding="utf-8", newline="")
        page_writer = csv.DictWriter(
            page_rows_file,
            fieldnames=[
                "page_id",
                "page_title",
                "revision_count",
                "total_citations",
                "avg_citations_per_revision",
                "max_citations_in_revision",
            ],
        )
        page_writer.writeheader()

    total_revisions = 0
    total_pages = 0
    total_citations = 0
    pages_with_zero_citations = 0
    missing_citation_count_records = 0
    recount_mismatch_records = 0

    current_page: Optional[PageStats] = None

    top_k_heap: List[Tuple[int, int, str]] = []

    def finalize_page(page: Optional[PageStats]) -> None:
        nonlocal total_pages, total_citations, pages_with_zero_citations
        if page is None:
            return

        total_pages += 1
        total_citations += page.total_citations
        if page.total_citations == 0:
            pages_with_zero_citations += 1

        maybe_write_page_row(page_writer, page)

        item = (page.total_citations, page.page_id, page.page_title)
        if args.top_k <= 0:
            return
        if len(top_k_heap) < args.top_k:
            heapq.heappush(top_k_heap, item)
        else:
            heapq.heappushpop(top_k_heap, item)

    stop_early = False
    for file_path in files:
        print(f"[INFO] Reading {file_path}")
        for record in read_records(file_path):
            page_id = record.get("page_id")
            page_title = record.get("page_title", "")

            if page_id is None:
                raise ValueError(f"Missing page_id in record from file {file_path}")

            if current_page is None or page_id != current_page.page_id:
                finalize_page(current_page)
                current_page = PageStats(page_id=int(page_id), page_title=str(page_title))

            citation_count = record.get("citation_count")
            if citation_count is None:
                missing_citation_count_records += 1
                citation_count = 0

            try:
                citation_count = int(citation_count)
            except (ValueError, TypeError):
                citation_count = 0

            current_page.revision_count += 1
            current_page.total_citations += citation_count
            if citation_count > current_page.max_citations_in_revision:
                current_page.max_citations_in_revision = citation_count

            if args.recount_from_raw:
                raw_text = record.get("raw_text", "")
                recount = count_citations_from_raw(str(raw_text))
                if recount != citation_count:
                    recount_mismatch_records += 1

            total_revisions += 1

            if args.progress_every > 0 and total_revisions % args.progress_every == 0:
                print(
                    f"[INFO] revisions={total_revisions:,} pages(finalized)={total_pages:,}"
                )

            if args.max_revisions and total_revisions >= args.max_revisions:
                stop_early = True
                break

        if stop_early:
            print(f"[INFO] Reached max revisions: {args.max_revisions:,}")
            break

    finalize_page(current_page)

    if page_rows_file is not None:
        page_rows_file.close()

    avg_citations_per_page = (total_citations / total_pages) if total_pages else 0.0
    avg_citations_per_revision = (total_citations / total_revisions) if total_revisions else 0.0

    print("\n=== Citation Sanity Check Summary ===")
    print(f"Files processed: {len(files)}")
    print(f"Pages: {total_pages:,}")
    print(f"Revisions: {total_revisions:,}")
    print(f"Total citations (sum citation_count): {total_citations:,}")
    print(f"Avg citations/page: {avg_citations_per_page:.4f}")
    print(f"Avg citations/revision: {avg_citations_per_revision:.4f}")
    print(f"Pages with zero citations: {pages_with_zero_citations:,}")
    print(f"Records missing citation_count: {missing_citation_count_records:,}")

    if args.recount_from_raw:
        print(f"Records mismatched vs recount(raw_text): {recount_mismatch_records:,}")

    if args.top_k > 0:
        top_pages = sorted(top_k_heap, reverse=True)
        print(f"\nTop {len(top_pages)} pages by total_citations:")
        for rank, (sum_citations, page_id, page_title) in enumerate(top_pages, 1):
            print(
                f"{rank:>2}. page_id={page_id} | total_citations={sum_citations:,} | title={page_title}"
            )

    if args.per_page_output:
        print(f"\nPer-page stats written to: {args.per_page_output}")


if __name__ == "__main__":
    main()
