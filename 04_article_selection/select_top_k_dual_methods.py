#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from statistics import mean, median


def configure_csv_field_size_limit() -> None:
	limit = sys.maxsize
	while True:
		try:
			csv.field_size_limit(limit)
			return
		except OverflowError:
			limit //= 10


def parse_args() -> argparse.Namespace:
	script_dir = Path(__file__).resolve().parent
	repo_root = script_dir.parent

	parser = argparse.ArgumentParser(
		description=(
			"Select top-k articles by two methods: "
			"(1) quality->citation and (2) weighted z-score final_score."
		)
	)
	parser.add_argument(
		"--input_csv",
		type=Path,
		default=None,
		help=(
			"Path to article_scores.csv. If omitted, script tries common locations in this repo."
		),
	)
	parser.add_argument(
		"--top_k",
		type=int,
		default=100000,
		help="Number of articles to keep for each method (default: 100000).",
	)
	parser.add_argument(
		"--w_quality",
		type=float,
		default=0.7,
		help="Weight for quality z-score in final_score (default: 0.7).",
	)
	parser.add_argument(
		"--w_citation",
		type=float,
		default=0.3,
		help="Weight for citation z-score in final_score (default: 0.3).",
	)
	parser.add_argument(
		"--output_dir",
		type=Path,
		default=repo_root / "ranking_results_cite",
		help="Directory for output files (default: ranking_results_cite).",
	)
	parser.add_argument(
		"--method1_output",
		type=str,
		default="top100k_method1_quality_then_citation.csv",
		help="Output filename for method 1 result.",
	)
	parser.add_argument(
		"--method2_output",
		type=str,
		default="top100k_method2_weighted_final_score.csv",
		help="Output filename for method 2 result.",
	)
	parser.add_argument(
		"--stats_file",
		type=str,
		default="top100k_selection_comparison_stats.txt",
		help="Output filename for method comparison statistics.",
	)
	parser.add_argument(
		"--raw_text_csv",
		type=Path,
		default=None,
		help=(
			"Path to latest_revisions_for_mysql.csv containing id/page_id and raw_text. "
			"If omitted, script tries common locations in this repo."
		),
	)

	args = parser.parse_args()
	if args.top_k <= 0:
		raise ValueError("--top_k must be > 0.")
	if args.w_quality < 0 or args.w_citation < 0:
		raise ValueError("--w_quality and --w_citation must be >= 0.")
	if args.w_quality == 0 and args.w_citation == 0:
		raise ValueError("At least one weight must be > 0.")

	args.candidate_inputs = [
		script_dir / "article_scores.csv",
		repo_root / "ranking_results_cite" / "article_scores.csv",
		repo_root / "ranking_results" / "article_scores.csv",
		repo_root / "03_ranking" / "outputs" / "article_scores.csv",
	]
	args.candidate_raw_text_inputs = [
		repo_root / "ranking_results_cite" / "latest_revisions_for_mysql.csv",
		repo_root / "ranking_results" / "latest_revisions_for_mysql.csv",
		script_dir / "latest_revisions_for_mysql.csv",
	]
	return args


def resolve_input_path(explicit_path: Path | None, candidates: list[Path]) -> Path:
	if explicit_path is not None:
		if explicit_path.exists():
			return explicit_path
		raise FileNotFoundError(f"Input file does not exist: {explicit_path}")

	for candidate in candidates:
		if candidate.exists():
			return candidate
	raise FileNotFoundError(
		"Could not find article_scores.csv in default locations. "
		"Please pass --input_csv explicitly."
	)


def load_rows(input_csv: Path) -> list[dict[str, str]]:
	with input_csv.open("r", encoding="utf-8", newline="") as f:
		reader = csv.DictReader(f)
		required_cols = {"page_id", "title", "quality_score", "citation_count"}
		missing = required_cols - set(reader.fieldnames or [])
		if missing:
			raise ValueError(
				f"Missing required columns: {sorted(missing)} in {input_csv}"
			)
		rows = list(reader)

	if not rows:
		raise ValueError(f"No data rows found in: {input_csv}")
	return rows


def load_raw_text_for_ids(raw_text_csv: Path, needed_ids: set[str]) -> dict[str, str]:
	if not needed_ids:
		return {}

	with raw_text_csv.open("r", encoding="utf-8", newline="") as f:
		reader = csv.DictReader(f)
		headers = set(reader.fieldnames or [])
		if "raw_text" not in headers:
			raise ValueError(f"Missing required column 'raw_text' in {raw_text_csv}")

		if "id" in headers:
			id_col = "id"
		elif "page_id" in headers:
			id_col = "page_id"
		else:
			raise ValueError(
				f"Missing id/page_id column in raw text file: {raw_text_csv}"
			)

		raw_map: dict[str, str] = {}
		for row in reader:
			pid = str(row.get(id_col, ""))
			if pid in needed_ids and pid not in raw_map:
				raw_map[pid] = row.get("raw_text", "")
				if len(raw_map) == len(needed_ids):
					break

	return raw_map


def normalize_weights(w_quality: float, w_citation: float) -> tuple[float, float]:
	total = w_quality + w_citation
	return w_quality / total, w_citation / total


def to_float(value: str, name: str, row_index: int) -> float:
	try:
		return float(value)
	except ValueError as exc:
		raise ValueError(
			f"Invalid {name} at row index {row_index}: {value!r}"
		) from exc


def page_id_sort_key(page_id: str) -> tuple[int, int | str]:
	if page_id.isdigit():
		return (0, int(page_id))
	return (1, page_id)


def mean_std(values: list[float]) -> tuple[float, float]:
	avg = mean(values)
	variance = sum((v - avg) ** 2 for v in values) / len(values)
	std = variance ** 0.5
	return avg, std


def summarize(values: list[float]) -> dict[str, float]:
	sorted_vals = sorted(values)
	count = len(sorted_vals)
	return {
		"count": float(count),
		"min": sorted_vals[0],
		"median": median(sorted_vals),
		"mean": mean(sorted_vals),
		"max": sorted_vals[-1],
	}


def method1_quality_then_citation(
	rows: list[dict[str, str]],
	top_k: int,
) -> tuple[list[dict[str, str]], dict[str, float]]:
	if top_k > len(rows):
		top_k = len(rows)

	rows_by_quality = sorted(
		rows,
		key=lambda r: (
			-float(r["quality_score"]),
			-float(r["citation_count"]),
			page_id_sort_key(r["page_id"]),
		),
	)

	quality_cutoff = float(rows_by_quality[top_k - 1]["quality_score"])
	quality_pool = [r for r in rows if float(r["quality_score"]) >= quality_cutoff]

	quality_pool_sorted = sorted(
		quality_pool,
		key=lambda r: (
			-float(r["citation_count"]),
			-float(r["quality_score"]),
			page_id_sort_key(r["page_id"]),
		),
	)
	selected = quality_pool_sorted[:top_k]

	citation_cutoff = float(selected[-1]["citation_count"])
	tie_at_cutoff_in_pool = sum(
		1 for r in quality_pool if float(r["citation_count"]) == citation_cutoff
	)

	stats = {
		"top_k": float(top_k),
		"quality_cutoff": quality_cutoff,
		"quality_pool_size": float(len(quality_pool)),
		"citation_cutoff_in_selected": citation_cutoff,
		"citation_ties_at_cutoff_in_pool": float(tie_at_cutoff_in_pool),
	}
	return selected, stats


def method2_weighted_final_score(
	rows: list[dict[str, str]],
	top_k: int,
	w_quality: float,
	w_citation: float,
) -> tuple[list[dict[str, str]], dict[str, float]]:
	if top_k > len(rows):
		top_k = len(rows)

	q_values = [float(r["quality_score"]) for r in rows]
	c_values = [float(r["citation_count"]) for r in rows]
	q_mean, q_std = mean_std(q_values)
	c_mean, c_std = mean_std(c_values)

	wq, wc = normalize_weights(w_quality, w_citation)

	rows_scored: list[dict[str, str]] = []
	for row in rows:
		q = float(row["quality_score"])
		c = float(row["citation_count"])
		zq = 0.0 if q_std == 0 else (q - q_mean) / q_std
		zc = 0.0 if c_std == 0 else (c - c_mean) / c_std
		final_score = wq * zq + wc * zc
		new_row = dict(row)
		new_row["final_score"] = f"{final_score:.10f}"
		rows_scored.append(new_row)

	rows_sorted = sorted(
		rows_scored,
		key=lambda r: (
			-float(r["final_score"]),
			-float(r["quality_score"]),
			-float(r["citation_count"]),
			page_id_sort_key(r["page_id"]),
		),
	)
	selected = rows_sorted[:top_k]

	final_cutoff = float(selected[-1]["final_score"])
	tie_at_cutoff = sum(1 for r in rows_scored if float(r["final_score"]) == final_cutoff)

	stats = {
		"top_k": float(top_k),
		"weight_quality_normalized": wq,
		"weight_citation_normalized": wc,
		"quality_mean": q_mean,
		"quality_std": q_std,
		"citation_mean": c_mean,
		"citation_std": c_std,
		"final_score_cutoff": final_cutoff,
		"final_score_ties_at_cutoff": float(tie_at_cutoff),
	}
	return selected, stats


def write_rows(output_csv: Path, rows: list[dict[str, str]]) -> None:
	if not rows:
		raise ValueError(f"Cannot write empty result to {output_csv}")

	output_csv.parent.mkdir(parents=True, exist_ok=True)
	fieldnames = list(rows[0].keys())
	with output_csv.open("w", encoding="utf-8", newline="") as f:
		writer = csv.DictWriter(f, fieldnames=fieldnames)
		writer.writeheader()
		writer.writerows(rows)


def write_stats_file(
	stats_file: Path,
	input_csv: Path,
	raw_text_csv: Path,
	total_rows: int,
	method1_stats: dict[str, float],
	method2_stats: dict[str, float],
	method1_rows: list[dict[str, str]],
	method2_rows: list[dict[str, str]],
	method1_raw_text_found: int,
	method2_raw_text_found: int,
) -> None:
	m1_quality = [float(r["quality_score"]) for r in method1_rows]
	m1_citation = [float(r["citation_count"]) for r in method1_rows]
	m2_quality = [float(r["quality_score"]) for r in method2_rows]
	m2_citation = [float(r["citation_count"]) for r in method2_rows]
	m2_final = [float(r["final_score"]) for r in method2_rows]

	m1_ids = {r["page_id"] for r in method1_rows}
	m2_ids = {r["page_id"] for r in method2_rows}
	intersection = len(m1_ids & m2_ids)

	m1_quality_stats = summarize(m1_quality)
	m1_citation_stats = summarize(m1_citation)
	m2_quality_stats = summarize(m2_quality)
	m2_citation_stats = summarize(m2_citation)
	m2_final_stats = summarize(m2_final)

	lines = [
		"Top-k article selection comparison",
		f"input_csv: {input_csv}",
		f"raw_text_csv: {raw_text_csv}",
		f"total_rows: {total_rows}",
		"",
		"Method 1: quality -> citation",
		f"top_k: {int(method1_stats['top_k'])}",
		f"quality_cutoff: {method1_stats['quality_cutoff']:.10f}",
		f"quality_pool_size: {int(method1_stats['quality_pool_size'])}",
		f"citation_cutoff_in_selected: {method1_stats['citation_cutoff_in_selected']:.10f}",
		f"citation_ties_at_cutoff_in_pool: {int(method1_stats['citation_ties_at_cutoff_in_pool'])}",
		f"selected_quality_mean: {m1_quality_stats['mean']:.10f}",
		f"selected_quality_median: {m1_quality_stats['median']:.10f}",
		f"selected_citation_mean: {m1_citation_stats['mean']:.10f}",
		f"selected_citation_median: {m1_citation_stats['median']:.10f}",
		f"raw_text_found_count: {method1_raw_text_found}",
		f"raw_text_found_ratio: {method1_raw_text_found / int(method1_stats['top_k']):.10f}",
		"",
		"Method 2: weighted z-score final_score",
		f"top_k: {int(method2_stats['top_k'])}",
		f"weight_quality_normalized: {method2_stats['weight_quality_normalized']:.10f}",
		f"weight_citation_normalized: {method2_stats['weight_citation_normalized']:.10f}",
		f"quality_mean_all: {method2_stats['quality_mean']:.10f}",
		f"quality_std_all: {method2_stats['quality_std']:.10f}",
		f"citation_mean_all: {method2_stats['citation_mean']:.10f}",
		f"citation_std_all: {method2_stats['citation_std']:.10f}",
		f"final_score_cutoff: {method2_stats['final_score_cutoff']:.10f}",
		f"final_score_ties_at_cutoff: {int(method2_stats['final_score_ties_at_cutoff'])}",
		f"selected_quality_mean: {m2_quality_stats['mean']:.10f}",
		f"selected_quality_median: {m2_quality_stats['median']:.10f}",
		f"selected_citation_mean: {m2_citation_stats['mean']:.10f}",
		f"selected_citation_median: {m2_citation_stats['median']:.10f}",
		f"selected_final_mean: {m2_final_stats['mean']:.10f}",
		f"selected_final_median: {m2_final_stats['median']:.10f}",
		f"raw_text_found_count: {method2_raw_text_found}",
		f"raw_text_found_ratio: {method2_raw_text_found / int(method2_stats['top_k']):.10f}",
		"",
		"Overlap",
		f"intersection_count: {intersection}",
		f"intersection_ratio_vs_topk: {intersection / int(method1_stats['top_k']):.10f}",
	]

	stats_file.parent.mkdir(parents=True, exist_ok=True)
	stats_file.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
	configure_csv_field_size_limit()
	args = parse_args()
	input_csv = resolve_input_path(args.input_csv, args.candidate_inputs)
	raw_text_csv = resolve_input_path(args.raw_text_csv, args.candidate_raw_text_inputs)
	rows = load_rows(input_csv)

	typed_rows: list[dict[str, str]] = []
	for idx, row in enumerate(rows, start=1):
		to_float(row["quality_score"], "quality_score", idx)
		to_float(row["citation_count"], "citation_count", idx)
		typed_rows.append(row)

	method1_rows, method1_stats = method1_quality_then_citation(typed_rows, args.top_k)
	method2_rows, method2_stats = method2_weighted_final_score(
		typed_rows,
		args.top_k,
		args.w_quality,
		args.w_citation,
	)
	needed_ids = {r["page_id"] for r in method1_rows} | {r["page_id"] for r in method2_rows}
	raw_text_map = load_raw_text_for_ids(raw_text_csv, needed_ids)

	for row in method1_rows:
		row["raw_text"] = raw_text_map.get(row["page_id"], "")
	for row in method2_rows:
		row["raw_text"] = raw_text_map.get(row["page_id"], "")

	method1_raw_text_found = sum(1 for row in method1_rows if row["raw_text"])
	method2_raw_text_found = sum(1 for row in method2_rows if row["raw_text"])

	method1_path = args.output_dir / args.method1_output
	method2_path = args.output_dir / args.method2_output
	stats_path = args.output_dir / args.stats_file

	write_rows(method1_path, method1_rows)
	write_rows(method2_path, method2_rows)
	write_stats_file(
		stats_path,
		input_csv,
		raw_text_csv,
		len(typed_rows),
		method1_stats,
		method2_stats,
		method1_rows,
		method2_rows,
		method1_raw_text_found,
		method2_raw_text_found,
	)

	print(f"Input CSV: {input_csv}")
	print(f"Raw text CSV: {raw_text_csv}")
	print(f"Total rows: {len(typed_rows)}")
	print(f"Top-k: {int(method1_stats['top_k'])}")
	print(
		"Method1 -> "
		f"quality_pool_size={int(method1_stats['quality_pool_size'])}, "
		f"quality_cutoff={method1_stats['quality_cutoff']:.10f}, "
		f"citation_cutoff={method1_stats['citation_cutoff_in_selected']:.10f}"
	)
	print(
		"Method2 -> "
		f"weights=({method2_stats['weight_quality_normalized']:.4f}, "
		f"{method2_stats['weight_citation_normalized']:.4f}), "
		f"final_score_cutoff={method2_stats['final_score_cutoff']:.10f}"
	)
	print(f"Method 1 output: {method1_path}")
	print(f"Method 2 output: {method2_path}")
	print(
		"Raw text coverage -> "
		f"method1={method1_raw_text_found}/{len(method1_rows)}, "
		f"method2={method2_raw_text_found}/{len(method2_rows)}"
	)
	print(f"Stats output: {stats_path}")


if __name__ == "__main__":
	main()