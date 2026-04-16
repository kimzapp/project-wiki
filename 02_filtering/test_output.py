import bz2
import json
import os

if __name__ == "__main__":
    with bz2.open("/home/rmits/project-wiki/02_filtering/outputs/viwiki-20251101-pages-meta-history3.xml-p3417395p3935700.jsonl.bz2", "rt") as file:
        for i, line in enumerate(file):
            data = json.loads(line)
            page_id = int(data.get("page_id"))
            page_title = data.get("title")
            revision_count = len(data.get("revisions", []))
            # print(f"Page ID: {page_id}, Title: {page_title}, Revision Count: {revision_count}")
            if page_id == 3526633:
                print(f"Page ID: {page_id}, Title: {page_title}, Revision Count: {revision_count}")
                raw_text = data["revisions"][-1].get("raw_text", "")
                print(f"Raw Text: {raw_text}...")  # Print the first 200 characters of raw_text for verification
                break