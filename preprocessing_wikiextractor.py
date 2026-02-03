import bz2
import xml.etree.ElementTree as ET
from lxml import etree
from wikiextractor.extract import Extractor


def clean_wikitext(wikitext, title, page_id):
    if not wikitext:
        return ""

    extractor = Extractor(
        id=page_id,
        title=title,
    )

    extractor.extract(wikitext)
    return "\n".join(extractor.text)

def parse_revisions(bz2_path):
    with bz2.open(bz2_path, "rb") as f:
        context = etree.iterparse(
            f,
            events=("end",),
            tag="{*}revision"
        )

        for _, revision in context:
            page = revision.getparent()

            page_id = page.findtext("{*}id")
            title = page.findtext("{*}title")

            rev_id = revision.findtext("{*}id")
            timestamp = revision.findtext("{*}timestamp")
            text = revision.findtext("{*}text")

            yield {
                "page_id": page_id,
                "title": title,
                "revision_id": rev_id,
                "timestamp": timestamp,
                "wikitext": text
            }

            # CỰC QUAN TRỌNG để tránh ăn RAM
            revision.clear()
            while revision.getprevious() is not None:
                del revision.getparent()[0]


for rev in parse_revisions("raw_histories/viwiki-20251101-pages-meta-history1.xml-p1p5762.bz2"):
    if not rev["wikitext"]:
        continue

    plain_text = clean_wikitext(
        rev["wikitext"],
        rev["title"],
        rev["page_id"]
    )

    record = {
        "page_id": rev["page_id"],
        "title": rev["title"],
        "revision_id": rev["revision_id"],
        "timestamp": rev["timestamp"],
        "text": plain_text
    }
    print(record)
    break

    # save JSONL / Parquet / DB

