import json

data_path = "/home/rmits/project-wiki/05_sentences/sampled_method1.jsonl"

with open(data_path, "r", encoding="utf-8") as f:
    for line in f.readlines():
        obj = json.loads(line)
        label = obj.get("label")
        if not label in [0, 1]:
            print(f"Invalid label: {label}")