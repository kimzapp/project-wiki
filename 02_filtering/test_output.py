import bz2
import json

if __name__ == "__main__":
    with bz2.open("/home/rmits/project-wiki/histories_filtered/viwiki-20251101-pages-meta-history1.xml-p1p5762.jsonl.bz2", "rt") as f:
        for i, line in enumerate(f):
            data = json.loads(line)
            page_id = data.get("page_id")
            page_title = data.get("title")
            revision_count = len(data.get("revisions", []))
            print(f"Page ID: {page_id}, Title: {page_title}, Revision Count: {revision_count}")