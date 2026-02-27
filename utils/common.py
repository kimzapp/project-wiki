import fcntl
from pathlib import Path
import os
import json
from typing import Tuple, Optional
from dataclasses import dataclass
import time


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
