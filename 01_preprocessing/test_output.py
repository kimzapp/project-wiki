import bz2
import json

if __name__ == "__main__":
    with bz2.open("/home/rmits/project-wiki/histories_cleaned/viwiki-20251101-pages-meta-history1.xml-p1p5762.jsonl.bz2", "rt") as f:
        for i, line in enumerate(f):
            data = json.loads(line)
            data.pop("timestamp", None)
            data.pop("user_name", None)
            data.pop("is_anonymous", None)  # Remove the empty key if it exists
            data.pop("is_bot", None)  # Remove the empty key if it exists

            print(data)
            if i >= 10:  # Only print first 5 lines for testing
                break