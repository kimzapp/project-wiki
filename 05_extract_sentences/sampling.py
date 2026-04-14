import json
import random
import argparse

# =========================
# CONFIG
# =========================
args = argparse.ArgumentParser(description="Sample balanced dataset from deduplicated text-label pairs.")
args.add_argument("--input", type=str, default="output_methodx_text_label_dedup.jsonl", help="Path to the deduplicated input JSONL file.")
args.add_argument("--output", type=str, default="balanced_dataset.jsonl", help="Path to save the balanced output JSONL file.")
args.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility.")
config = args.parse_args()  

INPUT_FILE = config.input
OUTPUT_FILE = config.output
SEED = config.seed

random.seed(SEED)

# =========================
# LOAD DATA
# =========================
class_1 = []
class_0 = []

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    for line in f:
        data = json.loads(line.strip())

        # đảm bảo có key
        if "label" not in data:
            continue

        if data["label"] == 1:
            class_1.append(data)
        elif data["label"] == 0:
            class_0.append(data)

print(f"Original class 1: {len(class_1)}")
print(f"Original class 0: {len(class_0)}")

# =========================
# SHUFFLE
# =========================
random.shuffle(class_1)
random.shuffle(class_0)

# =========================
# SAMPLE CLASS 1 (50%)
# =========================
n1_new = len(class_1) // 2
sampled_class_1 = class_1[:n1_new]

# =========================
# SAMPLE CLASS 0 (BALANCE)
# =========================
if len(class_0) >= n1_new:
    sampled_class_0 = class_0[:n1_new]
else:
    # edge case: class 0 không đủ -> oversample
    print("Warning: class 0 < required, performing oversampling...")
    sampled_class_0 = class_0.copy()
    while len(sampled_class_0) < n1_new:
        sampled_class_0.append(random.choice(class_0))

# =========================
# MERGE + SHUFFLE
# =========================
balanced_data = sampled_class_1 + sampled_class_0
random.shuffle(balanced_data)

print(f"Final dataset size: {len(balanced_data)}")
print(f"Class 1: {len(sampled_class_1)}")
print(f"Class 0: {len(sampled_class_0)}")

# =========================
# SAVE FILE
# =========================
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    for item in balanced_data:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")

print(f"Saved to {OUTPUT_FILE}")