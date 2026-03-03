import bz2
import json
from typing import Iterator, Dict, Any
from pathlib import Path

def read_jsonl_bz2(path: Path) -> Iterator[Dict[str, Any]]:
    """Stream read .jsonl.bz2 file"""
    with bz2.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


file_path = "/home/rmits/project-wiki/histories_cleaned/viwiki-20251101-pages-meta-history1.xml-p139313p273731.jsonl.bz2"


current_page_id = None
current_page_title = None
total_revision_count = 0
bot_revision_count = 0

for revision in read_jsonl_bz2(file_path):
        page_id = revision.pop("page_id")
        page_title = revision.pop("page_title", None)
        
        # khi chuyển sang page mới
        if page_id != current_page_id or page_title != current_page_title:
            print(f"Page ID: {page_id}, Title: {page_title} | Total revisions: {total_revision_count} | Total bot revisions: {bot_revision_count}")

            current_page_id = page_id
            current_page_title = page_title
            total_revision_count = 0
            bot_revision_count = 0

        # tiếp tục page hiện tại
        total_revision_count += 1
        
        # nếu là revision của bot thì bỏ qua và đếm số lượng bot revision đã bỏ qua
        if revision.get("is_bot", False):
            bot_revision_count += 1
            continue   