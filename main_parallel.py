# =====================================================
#               PARALLEL MAIN PIPELINE
# =====================================================
"""
Song song hoá pipeline xử lý Wikipedia dump files.

Cơ chế tránh xử lý trùng lặp:
- Mỗi file bz2 có một file marker `.done` khi hoàn thành
- Mỗi file đang xử lý có file `.lock` 
- Worker kiểm tra marker trước khi xử lý

Usage:
    # Chạy với 4 workers
    python main_parallel.py raw_histories -o outputs -w 4
    
    # Chạy nhiều instances (trên nhiều máy hoặc terminals)
    python main_parallel.py raw_histories -o outputs -w 2  # Terminal 1
    python main_parallel.py raw_histories -o outputs -w 2  # Terminal 2
"""

import argparse
import os
import fcntl
import time
import json
from pathlib import Path
from typing import List, Optional, Tuple
from multiprocessing import Pool, current_process
from dataclasses import dataclass

from utils.logging import setup_logging
from utils.bz2_stream import open_bz2_stream
from app.revision_processor import WikipediaRevisionProcessor


@dataclass
class ProcessingResult:
    """Kết quả xử lý một file."""
    file_path: str
    success: bool
    page_count: int
    revision_count: int
    elapsed_seconds: float
    error: Optional[str] = None


class FileLock:
    """
    File-based lock sử dụng fcntl để đảm bảo 
    chỉ một process xử lý một file tại một thời điểm.
    """
    def __init__(self, lock_path: Path):
        self.lock_path = lock_path
        self.lock_file = None
        
    def acquire(self) -> bool:
        """Thử acquire lock. Return True nếu thành công."""
        try:
            self.lock_file = open(self.lock_path, 'w')
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Ghi PID vào lock file để debug
            self.lock_file.write(f"{os.getpid()}\n")
            self.lock_file.flush()
            return True
        except (IOError, OSError):
            if self.lock_file:
                self.lock_file.close()
                self.lock_file = None
            return False
    
    def release(self):
        """Release lock."""
        if self.lock_file:
            try:
                fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
                self.lock_file.close()
            except:
                pass
            finally:
                self.lock_file = None
                # Xoá lock file
                try:
                    self.lock_path.unlink()
                except:
                    pass


def get_marker_paths(bz2_path: Path, marker_dir: Path) -> Tuple[Path, Path]:
    """Trả về đường dẫn của done marker và lock file."""
    base_name = bz2_path.name
    done_path = marker_dir / f"{base_name}.done"
    lock_path = marker_dir / f"{base_name}.lock"
    return done_path, lock_path


def is_completed(bz2_path: Path, marker_dir: Path) -> bool:
    """Kiểm tra xem file đã được xử lý xong chưa."""
    done_path, _ = get_marker_paths(bz2_path, marker_dir)
    return done_path.exists()


def mark_completed(bz2_path: Path, marker_dir: Path, result: ProcessingResult):
    """Đánh dấu file đã hoàn thành với metadata."""
    done_path, _ = get_marker_paths(bz2_path, marker_dir)
    metadata = {
        "file": bz2_path.name,
        "completed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "page_count": result.page_count,
        "revision_count": result.revision_count,
        "elapsed_seconds": result.elapsed_seconds,
        "pid": os.getpid(),
    }
    with open(done_path, 'w') as f:
        json.dump(metadata, f, indent=2)


def list_bz2_files(input_dir: str) -> List[Path]:
    """List tất cả file .bz2 trong thư mục."""
    p = Path(input_dir)
    if not p.exists():
        raise FileNotFoundError(f"Input path not found: {input_dir}")
    if not p.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {input_dir}")
    return sorted(
        [f for f in p.iterdir() if f.is_file() and f.suffix == ".bz2"],
        key=lambda x: x.name,
    )


def process_single_file(args: Tuple) -> ProcessingResult:
    """
    Xử lý một file bz2.
    Được gọi bởi worker process trong pool.
    """
    bz2_path, output_dir, marker_dir, log_dir, log_every = args
    bz2_path = Path(bz2_path)
    output_dir = Path(output_dir)
    marker_dir = Path(marker_dir)
    
    process_name = current_process().name
    start_time = time.time()
    
    # Kiểm tra đã hoàn thành chưa
    if is_completed(bz2_path, marker_dir):
        return ProcessingResult(
            file_path=str(bz2_path),
            success=True,
            page_count=0,
            revision_count=0,
            elapsed_seconds=0,
            error="Already completed (skipped)"
        )
    
    # Thử lấy lock
    _, lock_path = get_marker_paths(bz2_path, marker_dir)
    lock = FileLock(lock_path)
    
    if not lock.acquire():
        return ProcessingResult(
            file_path=str(bz2_path),
            success=False,
            page_count=0,
            revision_count=0,
            elapsed_seconds=0,
            error="Could not acquire lock (another process is handling)"
        )
    
    try:
        # Kiểm tra lại sau khi có lock (double-check)
        if is_completed(bz2_path, marker_dir):
            return ProcessingResult(
                file_path=str(bz2_path),
                success=True,
                page_count=0,
                revision_count=0,
                elapsed_seconds=0,
                error="Already completed (skipped after lock)"
            )
        
        # Setup logging cho worker này
        logger = setup_logging(
            log_dir=log_dir, 
            log_prefix=f"worker_{bz2_path.stem}"
        )
        logger.info(f"[{process_name}] Starting: {bz2_path.name}")
        
        # Output file riêng cho mỗi bz2 file
        output_path = output_dir / f"{bz2_path.stem}.jsonl"
        
        # Xử lý
        processor = WikipediaRevisionProcessor(
            output_path=str(output_path),
            log_every_n=log_every,
            max_pages=None,
            logger=logger,
        )
        
        with open_bz2_stream(bz2_path) as stream:
            processor.process(stream)
        
        elapsed = time.time() - start_time
        result = ProcessingResult(
            file_path=str(bz2_path),
            success=True,
            page_count=processor.page_count,
            revision_count=processor.revision_count,
            elapsed_seconds=elapsed,
        )
        
        # Đánh dấu hoàn thành
        mark_completed(bz2_path, marker_dir, result)
        
        logger.info(
            f"[{process_name}] Completed: {bz2_path.name} | "
            f"pages={processor.page_count:,} | "
            f"revisions={processor.revision_count:,} | "
            f"time={elapsed:.1f}s"
        )
        
        return result
        
    except Exception as e:
        elapsed = time.time() - start_time
        return ProcessingResult(
            file_path=str(bz2_path),
            success=False,
            page_count=0,
            revision_count=0,
            elapsed_seconds=elapsed,
            error=str(e)
        )
    finally:
        lock.release()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Parallel Wikipedia revision extraction pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    
    parser.add_argument(
        "input",
        help="Directory chứa các file Wikipedia XML.bz2 dump.",
    )
    
    parser.add_argument(
        "-o", "--output",
        default="outputs",
        help="Directory để lưu output JSONL files.",
    )
    
    parser.add_argument(
        "-w", "--workers",
        type=int,
        default=4,
        help="Số lượng worker processes.",
    )
    
    parser.add_argument(
        "--marker-dir",
        default=".markers",
        help="Directory để lưu file markers (done/lock).",
    )
    
    parser.add_argument(
        "--log-dir",
        default="logs",
        help="Directory để lưu log files.",
    )
    
    parser.add_argument(
        "--log-every",
        type=int,
        default=100_000,
        help="Log progress mỗi N revisions.",
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Setup directories
    output_dir = Path(args.output)
    marker_dir = Path(args.marker_dir)
    log_dir = Path(args.log_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    marker_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Main logger
    logger = setup_logging(log_dir=str(log_dir), log_prefix="main_parallel")
    
    # Lấy danh sách files
    bz2_files = list_bz2_files(args.input)
    logger.info(f"Found {len(bz2_files)} bz2 files")
    
    # Đếm files đã hoàn thành
    completed = sum(1 for f in bz2_files if is_completed(f, marker_dir))
    pending = len(bz2_files) - completed
    logger.info(f"Status: {completed} completed, {pending} pending")
    
    if pending == 0:
        logger.info("All files already processed!")
        return
    
    # Chuẩn bị args cho workers
    worker_args = [
        (str(f), str(output_dir), str(marker_dir), str(log_dir), args.log_every)
        for f in bz2_files
        if not is_completed(f, marker_dir)
    ]
    
    logger.info(f"Starting {args.workers} workers to process {len(worker_args)} files")
    start_time = time.time()
    
    # Xử lý song song
    with Pool(processes=args.workers) as pool:
        results = pool.map(process_single_file, worker_args)
    
    # Tổng kết
    total_elapsed = time.time() - start_time
    successful = [r for r in results if r.success and r.error is None]
    skipped = [r for r in results if r.success and r.error is not None]
    failed = [r for r in results if not r.success]
    
    total_pages = sum(r.page_count for r in results)
    total_revisions = sum(r.revision_count for r in results)
    
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total files: {len(results)}")
    logger.info(f"  Processed: {len(successful)}")
    logger.info(f"  Skipped: {len(skipped)}")
    logger.info(f"  Failed: {len(failed)}")
    logger.info(f"Total pages: {total_pages:,}")
    logger.info(f"Total revisions: {total_revisions:,}")
    logger.info(f"Total time: {total_elapsed:.1f}s")
    
    if failed:
        logger.error("Failed files:")
        for r in failed:
            logger.error(f"  {r.file_path}: {r.error}")


if __name__ == "__main__":
    main()
