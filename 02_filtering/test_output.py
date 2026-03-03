import bz2
import json

if __name__ == "__main__":
    with bz2.open("/home/rmits/project-wiki/02_filtering/outputs/viwiki-20251101-pages-meta-history1.xml-p139313p273731.jsonl.bz2", "rt") as f:
        for i, line in enumerate(f):
            data = json.loads(line)
            data.pop("timestamp", None)
            data.pop("user_name", None)
            data.pop("is_anonymous", None)  # Remove the empty key if it exists
            data.pop("is_bot", None)  # Remove the empty key if it exists

            print(data)
            break