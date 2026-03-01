#!/usr/bin/env python3
# =====================================================
#               SANITY CHECK PIPELINE
# =====================================================
"""
Script đơn giản để kiểm tra pipeline hoạt động đúng.

Kiểm tra:
1. Đọc được file bz2
2. Parse được XML
3. Trích xuất được revisions
4. Clean text hoạt động
5. Ghi được output JSONL

Usage:
    python sanity_check.py raw_histories
    python sanity_check.py raw_histories --max-pages 5
"""

import argparse
import json
import time
from pathlib import Path
from typing import Optional

from utils.bz2_stream import open_bz2_stream, strip_ns
from domain.cleaner import normalize_text
import xml.etree.ElementTree as ET


def sanity_check(
    input_dir: str,
    max_pages: int = 3,
    max_revisions_per_page: int = 5,
    output_file: Optional[str] = None,
):
    """
    Chạy sanity check trên một vài pages đầu tiên.
    """
    input_path = Path(input_dir)
    
    # Tìm file bz2 đầu tiên
    bz2_files = sorted(
        [f for f in input_path.iterdir() if f.suffix == ".bz2"],
        key=lambda x: x.name
    )
    
    if not bz2_files:
        print(f"❌ Không tìm thấy file .bz2 trong {input_dir}")
        return False
    
    bz2_file = bz2_files[0]
    print(f"📁 File test: {bz2_file.name}")
    print("=" * 60)
    
    # Test 1: Mở file bz2
    print("\n[1/5] Test đọc file bz2...")
    try:
        stream = open_bz2_stream(bz2_file)
        print("✅ Đọc file bz2 thành công")
    except Exception as e:
        print(f"❌ Lỗi đọc file bz2: {e}")
        return False
    
    # Test 2: Parse XML
    print("\n[2/5] Test parse XML...")
    try:
        context = ET.iterparse(stream, events=("end",))
        print("✅ Khởi tạo XML parser thành công")
    except Exception as e:
        print(f"❌ Lỗi khởi tạo XML parser: {e}")
        stream.close()
        return False
    
    # Test 3: Trích xuất pages và revisions
    print(f"\n[3/5] Test trích xuất {max_pages} pages đầu tiên...")
    
    page_count = 0
    revision_count = 0
    records = []
    start_time = time.time()
    
    try:
        for _, elem in context:
            if strip_ns(elem.tag) != "page":
                continue
            
            ns = elem.findtext("./{*}ns")
            if ns != "0":
                elem.clear()
                continue
            
            page_id = int(elem.findtext("./{*}id"))
            title = elem.findtext("./{*}title")
            
            print(f"\n  📄 Page {page_count + 1}: {title} (id={page_id})")
            
            revisions_in_page = 0
            for rev in elem.findall("./{*}revision"):
                if revisions_in_page >= max_revisions_per_page:
                    break
                    
                rev_id = rev.findtext("./{*}id")
                timestamp = rev.findtext("./{*}timestamp")
                raw_text = rev.findtext("./{*}text") or ""
                
                # Test 4: Clean text
                clean_text = normalize_text(raw_text)
                
                record = {
                    "page_id": page_id,
                    "title": title,
                    "revision_id": int(rev_id) if rev_id else None,
                    "timestamp": timestamp,
                    "text_preview": clean_text[:200] if clean_text else "(empty)",
                    "raw_length": len(raw_text),
                    "clean_length": len(clean_text) if clean_text else 0,
                }
                records.append(record)
                
                print(f"    📝 Revision {rev_id}: {timestamp}")
                print(f"       Raw: {len(raw_text)} chars → Clean: {len(clean_text) if clean_text else 0} chars")
                
                revisions_in_page += 1
                revision_count += 1
                rev.clear()
            
            elem.clear()
            page_count += 1
            
            if page_count >= max_pages:
                break
        
        elapsed = time.time() - start_time
        print(f"\n✅ Trích xuất thành công: {page_count} pages, {revision_count} revisions ({elapsed:.2f}s)")
        
    except Exception as e:
        print(f"❌ Lỗi trích xuất: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 5: Ghi output
    print("\n[4/5] Test ghi JSONL output...")
    
    output_path = Path(output_file) if output_file else Path("sanity_check_output.jsonl")
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        print(f"✅ Ghi {len(records)} records vào {output_path}")
    except Exception as e:
        print(f"❌ Lỗi ghi output: {e}")
        return False
    
    # Đọc lại để verify
    print("\n[5/5] Verify output file...")
    try:
        with open(output_path, 'r', encoding='utf-8') as f:
            read_count = sum(1 for _ in f)
        if read_count == len(records):
            print(f"✅ Verify thành công: {read_count} records")
        else:
            print(f"❌ Số records không khớp: wrote {len(records)}, read {read_count}")
            return False
    except Exception as e:
        print(f"❌ Lỗi verify: {e}")
        return False
    
    # Summary
    print("\n" + "=" * 60)
    print("🎉 TẤT CẢ TESTS PASSED!")
    print("=" * 60)
    print(f"\nSummary:")
    print(f"  - Pages processed: {page_count}")
    print(f"  - Revisions processed: {revision_count}")
    print(f"  - Output file: {output_path}")
    print(f"  - Time: {elapsed:.2f}s")
    
    # In sample record
    print(f"\n📋 Sample record:")
    print(json.dumps(records[0], indent=2, ensure_ascii=False))
    
    return True


def check_parallel_status(marker_dir: str = ".markers", input_dir: str = "raw_histories"):
    """Kiểm tra trạng thái xử lý song song."""
    marker_path = Path(marker_dir)
    input_path = Path(input_dir)
    
    if not marker_path.exists():
        print(f"⚠️  Marker directory không tồn tại: {marker_dir}")
        print("   (Chưa chạy parallel processing)")
        return
    
    # Đếm files
    bz2_files = [f for f in input_path.iterdir() if f.suffix == ".bz2"]
    done_files = [f for f in marker_path.iterdir() if f.suffix == ".done"]
    lock_files = [f for f in marker_path.iterdir() if f.suffix == ".lock"]
    
    print(f"\n📊 PARALLEL PROCESSING STATUS")
    print("=" * 60)
    print(f"Input directory: {input_dir}")
    print(f"Total bz2 files: {len(bz2_files)}")
    print(f"Completed: {len(done_files)}")
    print(f"In progress (locked): {len(lock_files)}")
    print(f"Pending: {len(bz2_files) - len(done_files)}")
    print(f"Progress: {len(done_files)}/{len(bz2_files)} ({100*len(done_files)/len(bz2_files):.1f}%)")
    
    # Chi tiết completed files
    if done_files:
        print(f"\n✅ Completed files ({len(done_files)}):")
        total_pages = 0
        total_revisions = 0
        for done_file in sorted(done_files)[:10]:  # Chỉ hiện 10 đầu
            try:
                with open(done_file) as f:
                    meta = json.load(f)
                print(f"   - {meta['file']}: {meta['page_count']:,} pages, {meta['revision_count']:,} revisions")
                total_pages += meta['page_count']
                total_revisions += meta['revision_count']
            except:
                print(f"   - {done_file.name}")
        
        if len(done_files) > 10:
            print(f"   ... và {len(done_files) - 10} files khác")
        
        print(f"\n   Total: {total_pages:,} pages, {total_revisions:,} revisions")
    
    # Lock files (có thể là stale)
    if lock_files:
        print(f"\n🔒 Locked files ({len(lock_files)}):")
        for lock_file in lock_files:
            try:
                with open(lock_file) as f:
                    pid = f.read().strip()
                print(f"   - {lock_file.stem} (PID: {pid})")
            except:
                print(f"   - {lock_file.stem}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Sanity check cho Wikipedia revision pipeline."
    )
    
    parser.add_argument(
        "input",
        nargs="?",
        default="raw_histories",
        help="Directory chứa các file bz2.",
    )
    
    parser.add_argument(
        "--max-pages",
        type=int,
        default=3,
        help="Số pages tối đa để test.",
    )
    
    parser.add_argument(
        "--max-revisions",
        type=int,
        default=5,
        help="Số revisions tối đa mỗi page.",
    )
    
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output file path.",
    )
    
    parser.add_argument(
        "--status",
        action="store_true",
        help="Chỉ kiểm tra trạng thái parallel processing.",
    )
    
    parser.add_argument(
        "--marker-dir",
        default=".markers",
        help="Marker directory (cho --status).",
    )
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    
    if args.status:
        check_parallel_status(args.marker_dir, args.input)
    else:
        success = sanity_check(
            input_dir=args.input,
            max_pages=args.max_pages,
            max_revisions_per_page=args.max_revisions,
            output_file=args.output,
        )
        exit(0 if success else 1)
