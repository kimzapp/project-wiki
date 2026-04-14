import argparse
import bz2
import csv
import multiprocessing as mp
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from prob_review import ProbReviewConfig, compute_page_scores
from utils import (
	FileLock,
	ProcessingResult,
	get_marker_paths,
	is_completed,
	list_input_files,
	mark_completed,
	preprocess_text,
	read_jsonl_bz2,
	safe_username,
	setup_logging,
	tokenize_text,
)


ARTICLE_FIELDS = [
	"page_id",
	"title",
	"quality_score",
	"n_tokens",
	"n_users",
	"iterations",
	"converged",
	"citation_count",
]

AUTHOR_PART_FIELDS = ["username", "score_sum", "pages"]


def parse_args() -> argparse.Namespace:
	"""Parse CLI options for ranking pipeline execution."""

	parser = argparse.ArgumentParser(
		description="Run ProbReview ranking on filtered Wikipedia revision histories.",
		formatter_class=argparse.ArgumentDefaultsHelpFormatter,
	)
	parser.add_argument(
		"--input_dir",
		type=Path,
		default=Path("../histories_filtered"),
		help="Directory containing filtered .jsonl.bz2 files.",
	)
	parser.add_argument(
		"--output_dir",
		type=Path,
		default=Path("outputs"),
		help="Directory for compressed output CSV files.",
	)
	parser.add_argument(
		"--parts_dir",
		type=Path,
		default=Path("outputs/parts"),
		help="Directory for per-input-file intermediate result files.",
	)
	parser.add_argument(
		"--marker_dir",
		type=Path,
		default=Path(".markers_ranking"),
		help="Directory storing .done/.lock markers for each input file.",
	)
	parser.add_argument(
		"--log_dir",
		type=Path,
		default=Path("logs"),
		help="Directory to store master + worker logs.",
	)
	parser.add_argument(
		"--workers",
		type=int,
		default=1,
		help="Number of worker processes. Each worker processes one input file at a time.",
	)
	parser.add_argument(
		"--scheme",
		type=str,
		default="S2",
		choices=["S1", "S2", "S3", "s1", "s2", "s3"],
		help="Probabilistic review decay scheme.",
	)
	parser.add_argument(
		"--alpha",
		type=float,
		default=7.0,
		help="Decay parameter for distance-based review probability.",
	)
	parser.add_argument(
		"--tol",
		type=float,
		default=1e-6,
		help="Convergence tolerance for iterative updates.",
	)
	parser.add_argument(
		"--max_iter",
		type=int,
		default=100,
		help="Maximum number of iterations.",
	)
	parser.add_argument(
		"--max_pages",
		type=int,
		default=None,
		help="Optional cap on processed pages per input file (for smoke tests).",
	)
	return parser.parse_args()


def prepare_page(page: Dict) -> Dict:
	"""Precompute per-revision username + token list used by ProbReview."""

	revisions = page.get("revisions", [])
	prepared_revisions: List[Dict] = []

	for rev in revisions:
		clean_text = rev.get("clean_text", "")
		normalized = preprocess_text(clean_text)
		tokens = tokenize_text(normalized)
		rev_copy = dict(rev)
		rev_copy["_username"] = safe_username(rev)
		rev_copy["_tokens"] = tokens
		prepared_revisions.append(rev_copy)

	# Ensure chronological order before lineage/review inference.
	prepared_revisions.sort(key=lambda r: r.get("timestamp", ""))
	return {
		"page_id": page.get("page_id"),
		"title": page.get("title", ""),
		"revisions": prepared_revisions,
	}


def write_csv_bz2(path: Path, fieldnames: List[str], rows: Iterable[Dict]) -> None:
	"""Write rows as compressed CSV (.csv.bz2)."""

	path.parent.mkdir(parents=True, exist_ok=True)
	with bz2.open(path, "wt", encoding="utf-8", newline="") as f:
		writer = csv.DictWriter(f, fieldnames=fieldnames)
		writer.writeheader()
		for row in rows:
			writer.writerow(row)


def read_csv_bz2(path: Path) -> List[Dict[str, str]]:
	"""Read compressed CSV (.csv.bz2) into list of dict rows."""

	with bz2.open(path, "rt", encoding="utf-8", newline="") as f:
		return list(csv.DictReader(f))


def process_input_file(
	input_file: Path,
	config: ProbReviewConfig,
	parts_dir: Path,
	marker_dir: Path,
	log_dir: Path,
	max_pages: int | None,
) -> ProcessingResult:
	"""Process one history shard file and write per-file article/author parts."""

	start = time.time()

	if is_completed(input_file, marker_dir):
		return ProcessingResult(
			file_path=str(input_file),
			success=True,
			processed_pages=0,
			article_rows=0,
			elapsed_seconds=0.0,
			error="Already completed (skipped)",
		)

	_, lock_path = get_marker_paths(input_file, marker_dir)
	lock = FileLock(lock_path)
	if not lock.acquire():
		return ProcessingResult(
			file_path=str(input_file),
			success=False,
			processed_pages=0,
			article_rows=0,
			elapsed_seconds=0.0,
			error="Could not acquire lock (another process handles this file)",
		)

	file_logger = setup_logging(log_dir=log_dir, log_prefix=f"worker_{input_file.stem}")
	file_logger.info("Start processing input file: %s", input_file.name)

	try:
		if is_completed(input_file, marker_dir):
			return ProcessingResult(
				file_path=str(input_file),
				success=True,
				processed_pages=0,
				article_rows=0,
				elapsed_seconds=0.0,
				error="Already completed (skipped after lock)",
			)

		article_rows: List[Dict] = []
		author_sum = defaultdict(float)
		author_count = defaultdict(int)

		processed_pages = 0
		for page in read_jsonl_bz2(input_file):
			# Prepare tokenized page then run ProbReview scoring.
			prepared = prepare_page(page)
			latest_citation_count = 0
			revisions = prepared.get("revisions", [])
			if revisions:
				try:
					latest_citation_count = int(revisions[-1].get("citation_count", 0) or 0)
				except (TypeError, ValueError):
					latest_citation_count = 0
			result = compute_page_scores(prepared, config)
			page_id = prepared.get("page_id")
			title = prepared.get("title", "")
			if result is None:
				file_logger.info(
					"Page done | page_id=%s | title=%s | status=SKIP_EMPTY",
					page_id,
					title,
				)
				continue

			article_rows.append(
				{
					"page_id": result.page_id,
					"title": result.title,
					"quality_score": f"{result.quality_score:.8f}",
					"n_tokens": result.n_tokens,
					"n_users": result.n_users,
					"iterations": result.iterations,
					"converged": result.converged,
					"citation_count": latest_citation_count,
				}
			)

			for username, score in result.user_scores.items():
				# Aggregate local author stats; merged globally at the end.
				author_sum[username] += score
				author_count[username] += 1

			processed_pages += 1
			file_logger.info(
				"Page done | page_id=%s | title=%s | tokens=%d | users=%d | score=%.6f | converged=%s",
				result.page_id,
				result.title,
				result.n_tokens,
				result.n_users,
				result.quality_score,
				result.converged,
			)
			if max_pages is not None and processed_pages >= max_pages:
				break

		article_rows.sort(key=lambda x: float(x["quality_score"]), reverse=True)
		# Persist per-file intermediates so final merge is restart-friendly.
		author_part_rows = [
			{
				"username": username,
				"score_sum": f"{author_sum[username]:.12f}",
				"pages": author_count[username],
			}
			for username in author_sum
		]

		base = input_file.stem
		article_part = parts_dir / f"{base}.article.csv.bz2"
		author_part = parts_dir / f"{base}.author.csv.bz2"
		write_csv_bz2(article_part, ARTICLE_FIELDS, article_rows)
		write_csv_bz2(author_part, AUTHOR_PART_FIELDS, author_part_rows)

		elapsed = time.time() - start
		result = ProcessingResult(
			file_path=str(input_file),
			success=True,
			processed_pages=processed_pages,
			article_rows=len(article_rows),
			elapsed_seconds=elapsed,
		)
		mark_completed(input_file, marker_dir, result)

		file_logger.info(
			"Finished %s | pages=%d | rows=%d | elapsed=%.2fs",
			input_file.name,
			processed_pages,
			len(article_rows),
			elapsed,
		)
		return result
	except Exception as exc:
		elapsed = time.time() - start
		file_logger.exception("Failed %s: %s", input_file.name, exc)
		return ProcessingResult(
			file_path=str(input_file),
			success=False,
			processed_pages=0,
			article_rows=0,
			elapsed_seconds=elapsed,
			error=str(exc),
		)
	finally:
		lock.release()


def merge_parts(input_files: List[Path], parts_dir: Path, output_dir: Path) -> Tuple[Path, Path]:
	"""Merge all per-file part outputs into final global article/author rankings."""

	article_all: List[Dict] = []
	author_sum = defaultdict(float)
	author_pages = defaultdict(int)

	for input_file in input_files:
		base = input_file.stem
		article_part = parts_dir / f"{base}.article.csv.bz2"
		author_part = parts_dir / f"{base}.author.csv.bz2"

		if article_part.exists():
			article_all.extend(read_csv_bz2(article_part))
		if author_part.exists():
			# Reconstruct global score sum and page count by author.
			for row in read_csv_bz2(author_part):
				username = row["username"]
				author_sum[username] += float(row["score_sum"])
				author_pages[username] += int(row["pages"])

	article_all.sort(key=lambda x: float(x["quality_score"]), reverse=True)
	# Final author score is average contribution over pages touched.
	author_rows = [
		{
			"username": username,
			"score": f"{(author_sum[username] / max(author_pages[username], 1)):.8f}",
			"pages": author_pages[username],
		}
		for username in author_sum
	]
	author_rows.sort(key=lambda x: float(x["score"]), reverse=True)

	article_out = output_dir / "article_scores.csv.bz2"
	author_out = output_dir / "author_scores.csv.bz2"
	write_csv_bz2(article_out, ARTICLE_FIELDS, article_all)
	write_csv_bz2(author_out, ["username", "score", "pages"], author_rows)
	return article_out, author_out


def worker_entry(args: Tuple) -> ProcessingResult:
	"""Pool-compatible wrapper for unpacking worker arguments."""

	return process_input_file(*args)


def log_file_result(logger, result: ProcessingResult, done: int, total: int) -> None:
	"""Emit one standardized progress line per completed input file."""

	status = "OK"
	if result.success and result.error is not None:
		status = "SKIP"
	elif not result.success:
		status = "FAIL"

	logger.info(
		"[%d/%d] %s | file=%s | pages=%d | rows=%d | time=%.2fs%s",
		done,
		total,
		status,
		Path(result.file_path).name,
		result.processed_pages,
		result.article_rows,
		result.elapsed_seconds,
		f" | reason={result.error}" if result.error else "",
	)


def main() -> None:
	"""Entry point: dispatch workers, collect results, and write merged outputs."""

	args = parse_args()
	args.output_dir.mkdir(parents=True, exist_ok=True)
	args.parts_dir.mkdir(parents=True, exist_ok=True)
	args.marker_dir.mkdir(parents=True, exist_ok=True)
	args.log_dir.mkdir(parents=True, exist_ok=True)

	logger = setup_logging(log_dir=args.log_dir, log_prefix="main_ranking")
	config = ProbReviewConfig(
		scheme=args.scheme.upper(),
		alpha=args.alpha,
		max_iter=args.max_iter,
		tol=args.tol,
	)

	input_files = list_input_files(args.input_dir)
	logger.info("Found %d input files", len(input_files))

	worker_args = [
		(input_file, config, args.parts_dir, args.marker_dir, args.log_dir, args.max_pages)
		for input_file in input_files
	]
	total_files = len(worker_args)

	start = time.time()
	if args.workers <= 1:
		# Deterministic single-process mode (useful for debugging).
		results = []
		for idx, item in enumerate(worker_args, 1):
			result = worker_entry(item)
			results.append(result)
			log_file_result(logger, result, idx, total_files)
	else:
		logger.info("Starting parallel processing with %d workers", args.workers)
		results = []
		with mp.Pool(processes=args.workers) as pool:
			# Unordered collection improves throughput for variable-size shards.
			for idx, result in enumerate(pool.imap_unordered(worker_entry, worker_args), 1):
				results.append(result)
				log_file_result(logger, result, idx, total_files)

	total_elapsed = time.time() - start
	successful = [r for r in results if r.success and r.error is None]
	skipped = [r for r in results if r.success and r.error is not None]
	failed = [r for r in results if not r.success]

	# Merge regardless of partial failures so completed parts are still materialized.
	article_out, author_out = merge_parts(input_files=input_files, parts_dir=args.parts_dir, output_dir=args.output_dir)

	logger.info("=" * 60)
	logger.info("SUMMARY")
	logger.info("processed files=%d | success=%d | skipped=%d | failed=%d", len(results), len(successful), len(skipped), len(failed))
	logger.info("elapsed=%.2fs", total_elapsed)
	logger.info("article output: %s", article_out)
	logger.info("author output: %s", author_out)

	if failed:
		logger.error("Failed files:")
		for item in failed:
			logger.error("%s -> %s", item.file_path, item.error)

	print(f"Wrote: {article_out}")
	print(f"Wrote: {author_out}")
	print(f"Processed files: {len(results)} | Success: {len(successful)} | Failed: {len(failed)}")


if __name__ == "__main__":
	main()
