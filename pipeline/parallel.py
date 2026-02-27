from typing import List, Tuple
import time
from pathlib import Path
from utils.common import is_completed, FileLock, mark_completed, get_marker_paths, ProcessingResult
from utils.bz2_stream import open_bz2_stream
from processor.revision_processor import WikipediaRevisionProcessor
from utils.logging import setup_logging
from multiprocessing import current_process


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