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

    def process(self, stream):
        context = ET.iterparse(stream, events=("end",))
        out = open(self.output_path, "a", encoding="utf-8")

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
                return  # 🔥 STOP GLOBAL

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

            self.logger.debug("Finished page_id=%d title=%s revision_count=%d", page_id, title, len(elem.findall("./{*}revision")))

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

        return RevisionRecord(
            page_id=page_id,
            page_title=title,
            revision_id=int(rev_id),
            parent_revision_id=int(prev_revision_id) if prev_revision_id else None,
            timestamp=timestamp,
            user_id=int(user_id) if user_id else None,
            username=username,
            is_anonymous=is_anonymous,
            comment=rev.findtext("./{*}comment") or "",
            raw_text_len=len(raw_text),
            clean_text_len=len(clean),
            clean_text=clean,
        )
