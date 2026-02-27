# =====================================================
#            APPLICATION / USE CASE
# =====================================================

import json
import time
from typing import Optional
from domain.models import RevisionRecord
from domain.cleaner import normalize_text
from utils.bz2_stream import strip_ns
import xml.etree.ElementTree as ET
import logging
import os
import bz2


BOT_LIST_PATH = "/home/rmits/project-wiki/bot_list_vi.txt"  # Đường dẫn đến file chứa danh sách bot

class WikipediaRevisionProcessor:
    def __init__(
        self,
        output_path: str,
        log_every_n: int,
        max_pages: Optional[int],
        logger: logging.Logger,
    ):
        self.output_path = output_path
        self.log_every_n = log_every_n
        self.max_pages = max_pages
        self.logger = logger

        self.page_count = 0
        self.revision_count = 0
        self.start_time = time.time()
        self.finished = False
        self.bot_list = self._load_bot_list()


    def _load_bot_list(self):
        """Load the list of known bot usernames from a file."""
        bot_list = set()
        try:
            with open(BOT_LIST_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    bot_list.add(line.strip())
        except FileNotFoundError:
            self.logger.warning(f"Bot list file not found: {BOT_LIST_PATH}")
        return bot_list


    def process(self, stream):
        context = ET.iterparse(stream, events=("end",))

        # 🔥 mở file bz2 ở text mode
        out = bz2.open(self.output_path, "at", encoding="utf-8")

        for _, elem in context:
            if strip_ns(elem.tag) != "page":
                continue

            ns = elem.findtext("./{*}ns")
            if ns != "0":
                elem.clear()
                continue

            if self.max_pages and self.page_count >= self.max_pages:
                self.logger.info(
                    "Reached max_pages=%d → stopping processing",
                    self.max_pages,
                )
                self.finished = True
                out.close()
                return

            page_id = int(elem.findtext("./{*}id"))
            title = elem.findtext("./{*}title")
            prev_revision_id = None

            for rev in elem.findall("./{*}revision"):
                record = self._parse_revision(
                    rev, page_id, title, prev_revision_id
                )
                if record is None:
                    rev.clear()
                    continue

                # ✅ ghi trực tiếp vào file nén
                out.write(json.dumps(record.__dict__, ensure_ascii=False) + "\n")

                prev_revision_id = record.revision_id
                self.revision_count += 1

                if self.revision_count % self.log_every_n == 0:
                    elapsed = max(time.time() - self.start_time, 1e-6)
                    self.logger.info(
                        f"pages={self.page_count:,} | "
                        f"revisions={self.revision_count:,} | "
                        f"speed={self.revision_count / elapsed:,.1f} rev/s"
                    )

                rev.clear()

            elem.clear()
            self.page_count += 1

        out.close()


    def _parse_revision(self, rev, page_id, title, prev_revision_id):
        rev_id = rev.findtext("./{*}id")
        timestamp = rev.findtext("./{*}timestamp")
        raw_text = rev.findtext("./{*}text") or ""

        clean = normalize_text(raw_text)
        if not clean or not timestamp:
            return None

        contributor = rev.find("./{*}contributor")
        user_id = username = None
        is_anonymous = False

        if contributor is not None:
            user_id = contributor.findtext("./{*}id")
            username = contributor.findtext("./{*}username")
            if username is None:
                username = contributor.findtext("./{*}ip")
                is_anonymous = True

        is_bot = self.detect_bot(username, rev.findtext("./{*}comment"))

        return RevisionRecord(
            page_id=page_id,
            page_title=title,
            revision_id=int(rev_id),
            parent_revision_id=int(prev_revision_id) if prev_revision_id else None,
            timestamp=timestamp,
            user_id=int(user_id) if user_id else None,
            username=username,
            is_anonymous=is_anonymous,
            is_bot=is_bot,
            raw_text_len=len(raw_text),
            clean_text_len=len(clean),
            clean_text=clean,
        )

    def detect_bot(self, username: Optional[str], comment: Optional[str] = None) -> bool:
        """Phát hiện bot dựa trên username, comment và danh sách bot đã tải về."""
        if not username and not comment:
            return False
        
        if username in self.bot_list:
            return True

        username = username.lower() if username else ""
        comment = comment.lower() if comment else ""
        bot_indicators = ["bot", "auto", "script", "crawler", "spider"]
        return any(ind in username or ind in comment for ind in bot_indicators)
    

if __name__ == "__main__":
    def _load_bot_list():
        """Load the list of known bot usernames from a file."""
        bot_list = set()
        try:
            with open(BOT_LIST_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    bot_list.add(line.strip())
        except Exception as e:
            print(f"Error loading bot list: {e}")
        return bot_list
    
    print(_load_bot_list())