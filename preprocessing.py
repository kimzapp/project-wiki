import sys
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime
import time
import bz2
import mwparserfromhell
from gensim.corpora.wikicorpus import filter_wiki

# =====================================================
#              BZ2 STREAM HELPER
# =====================================================

def open_bz2_stream(bz2_path):
    return bz2.open(bz2_path, mode="rt", encoding="utf-8", errors="ignore")


# =====================================================
#                       CLEANERS
# =====================================================

BAD_TEMPLATES = {
    "fact", "citation needed", "clarify", "cn",
    "coord", "refn", "birth date", "death date and age", "cờ", "flagicon"
}

def strip_ns(tag):
    return tag.split("}", 1)[-1]

def normalize_text(text: str) -> str:
    if not text:
        return ""

    clean_text = filter_wiki(text).strip()

    # wikicode = mwparserfromhell.parse(text)

    # # --------------------------------------------------
    # # Remove file blocks [[File:...]], [[Image:...]],...
    # # --------------------------------------------------
    # for link in wikicode.filter_wikilinks(recursive=True):
    #     if link.title.lower().startswith(('image:', 'file:', 'media:', 'ảnh:', 'tập tin:', 'hình ảnh:')):
    #         wikicode.remove(link)

    # # --------------------------------------------------
    # # Remove bad templates explicitly
    # # --------------------------------------------------
    # for tpl in wikicode.filter_templates(recursive=True):
    #     name = tpl.name.strip().lower()
    #     print("Template found:", name)
    #     if name in BAD_TEMPLATES:
    #         wikicode.remove(tpl)

    # # --------------------------------------------------
    # # Remove templates {{...}}
    # # --------------------------------------------------
    # for tpl in wikicode.filter_templates(recursive=True):
    #     wikicode.remove(tpl)

    # # --------------------------------------------------
    # # Final cleanup
    # # --------------------------------------------------
    # clean_text = wikicode.strip_code()
    # clean_text = re.sub(r'\s+', ' ', clean_text).strip()

    return clean_text

# =====================================================
#                 REVISION PIPELINE
# =====================================================

def process_stream(
    stream,
    out_path="wiki_revisions_clean.jsonl",
    log_every_n=100_000,
):
    context = ET.iterparse(stream, events=("end",))
    out = open(out_path, "w", encoding="utf-8")

    revision_count = 0
    page_count = 0
    start_time = time.time()
    last_log_time = start_time

    for _, elem in context:
        if strip_ns(elem.tag) != "page":
            continue

        ns = elem.findtext("./{*}ns")
        if ns != "0":
            elem.clear()
            continue

        page_count += 1
        page_id = elem.findtext("./{*}id")
        title = elem.findtext("./{*}title")

        prev_revision_id = None

        for rev in elem.findall("./{*}revision"):
            rev_id = rev.findtext("./{*}id")
            timestamp = rev.findtext("./{*}timestamp")
            comment = rev.findtext("./{*}comment") or ""

            contributor = rev.find("./{*}contributor")
            user_id = None
            username = None
            is_anonymous = False

            if contributor is not None:
                user_id = contributor.findtext("./{*}id")
                username = contributor.findtext("./{*}username")
                if username is None:
                    username = contributor.findtext("./{*}ip")
                    is_anonymous = True

            raw_text = rev.findtext("./{*}text") or ""
            clean = normalize_text(raw_text)

            if not clean or not timestamp:
                rev.clear()
                continue

            record = {
                "page_id": int(page_id),
                "page_title": title,
                "revision_id": int(rev_id),
                "parent_revision_id": int(prev_revision_id) if prev_revision_id else None,
                "timestamp": timestamp,
                "user_id": int(user_id) if user_id else None,
                "username": username,
                "is_anonymous": is_anonymous,
                "comment": comment,
                "raw_text_len": len(raw_text),
                "clean_text_len": len(clean),
                "clean_text": clean,
            }

            out.write(json.dumps(record, ensure_ascii=False) + "\n")

            prev_revision_id = rev_id
            revision_count += 1

            if revision_count % log_every_n == 0:
                now = time.time()
                speed = revision_count / max(now - start_time, 1e-6)
                print(
                    f"[INFO] pages={page_count:,} | "
                    f"revisions={revision_count:,} | "
                    f"speed={speed:,.1f} rev/s",
                    flush=True
                )

            rev.clear()

        elem.clear()
        
        print(f"[DEBUG] Finished processing page_id={page_id}, title={title}, revisions count={revision_count}")
        # break # Remove this line to process the entire stream

    out.close()


# =====================================================
#                     MAIN
# =====================================================

if __name__ == "__main__":
    bz2_path = sys.argv[1]
    print(bz2_path)
    output_path = 'wiki_revisions_clean.jsonl'
    stream = open_bz2_stream(bz2_path)
    process_stream(stream, out_path=output_path)