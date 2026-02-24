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
import time
from pathlib import Path
from multiprocessing import Pool
from utils.logging import setup_logging
from utils.parallel import is_completed
from processing.parallel import process_single_file, list_bz2_files


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
