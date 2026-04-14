import bz2
import fcntl
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import numpy as np


WORD_RE = re.compile(r"\w+", flags=re.UNICODE)


def read_jsonl_bz2(path: Path) -> Iterator[Dict[str, Any]]:
	"""Stream a .jsonl.bz2 file and yield parsed JSON objects."""
	with bz2.open(path, "rt", encoding="utf-8") as f:
		for line in f:
			line = line.strip()
			if not line:
				continue
			yield json.loads(line)


def preprocess_text(text: str) -> str:
	"""Normalize text before tokenization while preserving token order."""
	if not text:
		return ""
	# Basic normalization: lowercase and collapse whitespace.
	text = text.lower()
	text = re.sub(r"\s+", " ", text).strip()
	return text


def tokenize_text(text: str) -> List[str]:
	"""Tokenize text into ordered word tokens."""
	if not text:
		return []
	return WORD_RE.findall(text)


def l1_normalize(vec: np.ndarray) -> np.ndarray:
	"""L1-normalize a vector, returning zeros when the norm is zero."""
	denom = np.sum(np.abs(vec))
	if denom <= 0:
		# Keep shape/value (all zeros) instead of dividing by zero.
		return vec
	return vec / denom


def safe_username(revision: Dict[str, Any]) -> str:
	"""Extract a stable username key from a revision."""
	username = revision.get("username")
	if username:
		return str(username)
	user_id = revision.get("user_id")
	if user_id is not None:
		# Fallback keeps anonymous/missing-name edits distinguishable.
		return f"user_id:{user_id}"
	return "<unknown>"


def list_input_files(input_dir: Path) -> List[Path]:
	"""Return sorted input .jsonl.bz2 files."""
	return sorted(input_dir.glob("*.jsonl.bz2"))


@dataclass
class ProcessingResult:
	"""Outcome of processing one input history file."""
	file_path: str
	success: bool
	processed_pages: int
	article_rows: int
	elapsed_seconds: float
	error: Optional[str] = None


class FileLock:
	"""Non-blocking file lock used to avoid duplicate processing across workers."""

	def __init__(self, lock_path: Path):
		self.lock_path = lock_path
		self.lock_file = None

	def acquire(self) -> bool:
		"""Try to acquire exclusive lock; return False immediately if unavailable."""

		try:
			self.lock_path.parent.mkdir(parents=True, exist_ok=True)
			self.lock_file = open(self.lock_path, "w", encoding="utf-8")
			fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
			self.lock_file.write(f"{os.getpid()}\n")
			self.lock_file.flush()
			return True
		except (IOError, OSError):
			if self.lock_file:
				self.lock_file.close()
				self.lock_file = None
			return False

	def release(self) -> None:
		"""Best-effort unlock + cleanup of lock file."""

		if self.lock_file:
			try:
				fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
				self.lock_file.close()
			except Exception:
				pass
			finally:
				self.lock_file = None
				try:
					self.lock_path.unlink()
				except Exception:
					pass


def get_marker_paths(input_file: Path, marker_dir: Path) -> Tuple[Path, Path]:
	"""Return companion done/lock marker paths for one input file."""

	base = input_file.name
	done_path = marker_dir / f"{base}.done"
	lock_path = marker_dir / f"{base}.lock"
	return done_path, lock_path


def is_completed(input_file: Path, marker_dir: Path) -> bool:
	"""Check whether input file already has a completion marker."""

	done_path, _ = get_marker_paths(input_file, marker_dir)
	return done_path.exists()


def mark_completed(input_file: Path, marker_dir: Path, result: ProcessingResult) -> None:
	"""Write completion metadata for restart-safe incremental processing."""

	done_path, _ = get_marker_paths(input_file, marker_dir)
	metadata = {
		"file": input_file.name,
		"completed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
		"processed_pages": result.processed_pages,
		"article_rows": result.article_rows,
		"elapsed_seconds": result.elapsed_seconds,
		"pid": os.getpid(),
	}
	with done_path.open("w", encoding="utf-8") as f:
		json.dump(metadata, f, ensure_ascii=False, indent=2)


def setup_logging(log_dir: Path, log_prefix: str) -> logging.Logger:
	"""Create per-process logger with dedicated file + console output."""
	log_dir.mkdir(parents=True, exist_ok=True)
	timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
	pid = os.getpid()
	log_file = log_dir / f"{log_prefix}_{timestamp}_{pid}.log"

	logger = logging.getLogger(f"ranking-{log_prefix}-{pid}")
	if logger.handlers:
		# Reuse existing logger when function is called repeatedly in same process.
		return logger

	logger.setLevel(logging.DEBUG)
	formatter = logging.Formatter(
		"%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
	)

	console = logging.StreamHandler()
	console.setLevel(logging.INFO)
	console.setFormatter(formatter)

	file_handler = logging.FileHandler(log_file, encoding="utf-8")
	file_handler.setLevel(logging.DEBUG)
	file_handler.setFormatter(formatter)

	logger.addHandler(console)
	logger.addHandler(file_handler)
	logger.info("Logger initialized: %s", log_file)
	return logger
