from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Dict, List, Sequence, Tuple

import numpy as np

try:
	from scipy.sparse import csr_matrix
except Exception as exc:  # pragma: no cover
	raise ImportError(
		"scipy is required for ProbReview sparse computation. "
		"Install it with: pip install scipy"
	) from exc

from utils import l1_normalize


@dataclass
class ProbReviewConfig:
	"""Hyperparameters controlling decay and fixed-point convergence."""

	scheme: str = "S2"
	alpha: float = 7.0
	max_iter: int = 100
	tol: float = 1e-6


@dataclass
class PageScoreResult:
	"""Final per-page outputs: article quality and per-user contribution scores."""

	page_id: int
	title: str
	n_tokens: int
	n_users: int
	iterations: int
	converged: bool
	quality_score: float
	user_scores: Dict[str, float]


def _scheme_s1(distance: int, alpha: float) -> float:
	# Exponential decay: influence drops quickly as distance increases.
	return np.exp(-abs(distance) / max(alpha, 1e-12))


def _scheme_s2(distance: int, alpha: float) -> float:
	# Piecewise-linear tail around alpha, then harmonic falloff.
	return 1.0 / (max(abs(distance) - alpha, 0.0) + 1.0)


def _scheme_s3(distance: int, alpha: float) -> float:
	# Cauchy-like heavy tail: more tolerant to larger distances.
	return 1.0 / (1.0 + (abs(distance) / max(alpha, 1e-12)) ** 2)


def decay_probability(distance: int, scheme: str, alpha: float) -> float:
	"""Convert edit distance to review probability using selected decay scheme."""

	scheme = scheme.upper()
	if scheme == "S1":
		return float(_scheme_s1(distance, alpha))
	if scheme == "S2":
		return float(_scheme_s2(distance, alpha))
	if scheme == "S3":
		return float(_scheme_s3(distance, alpha))
	raise ValueError(f"Unsupported scheme: {scheme}")


def _merge_prob(old: float, new: float) -> float:
	"""Aggregate repeated review events with independent-event approximation."""
	return 1.0 - (1.0 - old) * (1.0 - new)


def _edit_anchor_positions(opcodes: Sequence[Tuple[str, int, int, int, int]]) -> List[int]:
	"""Return anchor positions in the current revision for changed regions."""
	anchors: List[int] = []
	for tag, _i1, _i2, j1, j2 in opcodes:
		if tag == "equal":
			continue
		if j2 > j1:
			anchors.extend(range(j1, j2))
		else:
			anchors.append(j1)
	return anchors


def _min_distance(idx: int, anchors: Sequence[int]) -> int:
	"""Smallest distance from token index to any edited anchor."""

	return min(abs(idx - a) for a in anchors)


def compute_page_scores(
	page: Dict,
	config: ProbReviewConfig,
) -> PageScoreResult | None:
	"""
	ProbReview core pipeline for one page.

	1) Track token lineage across revisions.
	2) Estimate review probabilities from edit proximity.
	3) Build token-user interaction matrix.
	4) Solve coupled token/user scores by fixed-point iteration.
	"""

	revisions = page.get("revisions", [])
	if not revisions:
		return None

	# Token occurrence state for current revision.
	prev_tokens: List[str] = []
	prev_occ_ids: List[int] = []

	# Lineage and interaction accumulators.
	occ_author: Dict[int, str] = {}
	occ_reviewers: Dict[int, Dict[str, float]] = {}
	next_occ_id = 0

	for rev in revisions:
		username = str(rev.get("_username", "<unknown>"))
		curr_tokens = rev.get("_tokens", [])
		if not isinstance(curr_tokens, list):
			curr_tokens = []

		# Diff previous and current token sequence to map survival/new/deleted tokens.
		sm = SequenceMatcher(a=prev_tokens, b=curr_tokens, autojunk=False)
		opcodes = sm.get_opcodes()
		anchors = _edit_anchor_positions(opcodes)

		curr_occ_ids: List[int] = []
		for tag, i1, i2, j1, j2 in opcodes:
			if tag == "equal":
				# Unchanged tokens keep their original occurrence ids (lineage continuity).
				curr_occ_ids.extend(prev_occ_ids[i1:i2])
			elif tag in ("replace", "insert"):
				# New/rewritten tokens get new occurrence ids owned by current editor.
				for _ in range(j1, j2):
					occ_id = next_occ_id
					next_occ_id += 1
					occ_author[occ_id] = username
					occ_reviewers[occ_id] = {}
					curr_occ_ids.append(occ_id)
			elif tag == "delete":
				continue
			else:  # pragma: no cover
				raise ValueError(f"Unexpected diff opcode: {tag}")

		# Review signals for unchanged tokens in current revision.
		if anchors:
			for tag, i1, i2, j1, j2 in opcodes:
				if tag != "equal":
					continue
				block_len = i2 - i1
				for offset in range(block_len):
					curr_idx = j1 + offset
					occ_id = prev_occ_ids[i1 + offset]
					d = _min_distance(curr_idx, anchors)
					p = decay_probability(d, config.scheme, config.alpha)
					if p <= 0:
						continue
					# Same reviewer can touch nearby edits multiple times; merge probabilities.
					old = occ_reviewers[occ_id].get(username, 0.0)
					occ_reviewers[occ_id][username] = _merge_prob(old, p)

		prev_tokens = curr_tokens
		prev_occ_ids = curr_occ_ids

	final_occ_ids = prev_occ_ids
	if not final_occ_ids:
		return None

	users = sorted(
		{
			occ_author[occ_id]
			for occ_id in final_occ_ids
			if occ_id in occ_author
		}
		| {
			u
			for occ_id in final_occ_ids
			for u in occ_reviewers.get(occ_id, {}).keys()
		}
	)
	if not users:
		return None

	user_to_col = {u: i for i, u in enumerate(users)}
	occ_to_row = {occ_id: i for i, occ_id in enumerate(final_occ_ids)}

	data: List[float] = []
	rows: List[int] = []
	cols: List[int] = []

	for occ_id in final_occ_ids:
		row = occ_to_row[occ_id]
		author = occ_author[occ_id]
		# Author-token edge has base weight 1.0.
		rows.append(row)
		cols.append(user_to_col[author])
		data.append(1.0)

		for reviewer, p in occ_reviewers.get(occ_id, {}).items():
			if reviewer not in user_to_col:
				continue
			rows.append(row)
			cols.append(user_to_col[reviewer])
			data.append(float(p))

	f_matrix = csr_matrix((data, (rows, cols)), shape=(len(final_occ_ids), len(users)))

	# Fixed-point iteration with L1 normalization keeps vectors comparable and stable.
	a = np.ones(len(users), dtype=np.float64)
	a = l1_normalize(a)
	q = np.ones(len(final_occ_ids), dtype=np.float64)
	q = l1_normalize(q)

	converged = False
	iterations = 0
	for it in range(1, config.max_iter + 1):
		# Token quality receives user authority mass.
		q_new = f_matrix.dot(a)
		q_new = l1_normalize(np.asarray(q_new).reshape(-1))

		# User authority receives mass back from token quality.
		a_new = f_matrix.T.dot(q_new)
		a_new = l1_normalize(np.asarray(a_new).reshape(-1))

		# Stop when both coupled vectors stop changing significantly.
		dq = np.linalg.norm(q_new - q, ord=1)
		da = np.linalg.norm(a_new - a, ord=1)
		q, a = q_new, a_new
		iterations = it

		if dq < config.tol and da < config.tol:
			converged = True
			break

	# q is L1-normalized for stable iteration; use a raw pass for article-level magnitude.
	q_raw = f_matrix.dot(a)
	quality_score = float(np.sum(np.asarray(q_raw).reshape(-1)))
	user_scores = {u: float(a[user_to_col[u]]) for u in users}

	return PageScoreResult(
		page_id=int(page.get("page_id", -1)),
		title=str(page.get("title", "")),
		n_tokens=len(final_occ_ids),
		n_users=len(users),
		iterations=iterations,
		converged=converged,
		quality_score=quality_score,
		user_scores=user_scores,
	)
