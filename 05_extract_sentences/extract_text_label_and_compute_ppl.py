import argparse
import json
import math
import sqlite3
from pathlib import Path
from typing import Any

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer


BASE_DIR = Path(__file__).resolve().parent


def extract_text_label(input_path: Path, output_path: Path) -> tuple[int, int]:
    page_count = 0
    sentence_count = 0

    with input_path.open("r", encoding="utf-8") as src, output_path.open("w", encoding="utf-8") as dst:
        for line in src:
            line = line.strip()
            if not line:
                continue

            page = json.loads(line)
            page_count += 1

            for paragraph in page.get("paragraphs", []):
                for sentence in paragraph.get("sentences", []):
                    text = sentence.get("text", "")
                    label = sentence.get("label")
                    dst.write(
                        json.dumps({"text": text, "label": label}, ensure_ascii=False) + "\n"
                    )
                    sentence_count += 1

    return page_count, sentence_count


def deduplicate_text_label(input_path: Path, output_path: Path) -> tuple[int, int]:
    db_path = BASE_DIR / f".{input_path.stem}_dedup.sqlite3"
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(
            """
            CREATE TABLE sentence_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT NOT NULL,
                label INTEGER,
                UNIQUE(text, label)
            )
            """
        )

        total_in = 0
        with input_path.open("r", encoding="utf-8") as src:
            for line in src:
                line = line.strip()
                if not line:
                    continue

                record = json.loads(line)
                conn.execute(
                    "INSERT OR IGNORE INTO sentence_records(text, label) VALUES (?, ?)",
                    (record.get("text", ""), record.get("label")),
                )
                total_in += 1

                if total_in % 10000 == 0:
                    conn.commit()

        conn.commit()

        with output_path.open("w", encoding="utf-8") as dst:
            for text, label in conn.execute(
                "SELECT text, label FROM sentence_records ORDER BY id"
            ):
                dst.write(json.dumps({"text": text, "label": label}, ensure_ascii=False) + "\n")

        unique_count = conn.execute("SELECT COUNT(*) FROM sentence_records").fetchone()[0]
    finally:
        conn.close()
        if db_path.exists():
            db_path.unlink()

    return total_in, unique_count


class BatchedPerplexityScorer:
    def __init__(
        self,
        model_name: str,
        max_length: int,
        force_cpu: bool,
    ) -> None:
        self.max_length = max_length
        self.device = torch.device("cpu" if force_cpu or not torch.cuda.is_available() else "cuda")
        torch_dtype = torch.float16 if self.device.type == "cuda" else torch.float32

        self.tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token
        self.tokenizer.padding_side = "right"

        self.model = AutoModelForCausalLM.from_pretrained(
            model_name,
            torch_dtype=torch_dtype,
            trust_remote_code=True,
        )
        if getattr(self.model.config, "pad_token_id", None) is None:
            self.model.config.pad_token_id = self.tokenizer.pad_token_id
        self.model = self.model.to(self.device)
        self.model.eval()

    @torch.no_grad()
    def score_texts(self, texts: list[str]) -> list[float]:
        if not texts:
            return []

        encoded = self.tokenizer(
            texts,
            return_tensors="pt",
            truncation=True,
            max_length=self.max_length,
            padding=True,
        )
        input_ids = encoded["input_ids"].to(self.device)
        attention_mask = encoded["attention_mask"].to(self.device)

        labels = input_ids.clone()
        labels[attention_mask == 0] = -100

        # Some PhoGPT implementations can raise shape errors when attention_mask
        # has padded batch dimensions; masking labels avoids that path.
        outputs = self.model(input_ids=input_ids)
        logits = outputs.logits[:, :-1, :]
        shift_labels = labels[:, 1:]
        valid_mask = (shift_labels != -100)
        token_mask = valid_mask.to(logits.dtype)

        log_probs = torch.log_softmax(logits, dim=-1)
        gather_labels = shift_labels.masked_fill(~valid_mask, 0)
        token_log_probs = log_probs.gather(dim=2, index=gather_labels.unsqueeze(-1)).squeeze(-1)
        token_nll = -token_log_probs * token_mask

        token_count = token_mask.sum(dim=1)
        sequence_loss = token_nll.sum(dim=1) / token_count.clamp(min=1)
        perplexities = torch.exp(sequence_loss)

        scores: list[float] = []
        for idx, ppl_tensor in enumerate(perplexities):
            if token_count[idx].item() == 0:
                scores.append(float("inf"))
                continue

            score = float(ppl_tensor.item())
            if math.isnan(score) or math.isinf(score):
                score = float("inf")
            scores.append(score)
        return scores


def _is_cuda_oom_error(err: RuntimeError, device: torch.device) -> bool:
    if device.type != "cuda":
        return False
    err_msg = str(err).lower()
    return "out of memory" in err_msg or "cuda error" in err_msg


def add_perplexity_scores(
    input_path: Path,
    output_path: Path,
    scorer: BatchedPerplexityScorer,
    batch_size: int,
) -> tuple[int, int]:
    processed = 0
    failed = 0
    current_batch_size = max(1, batch_size)

    def process_chunk(records: list[dict[str, Any]], out_handle: Any, local_batch_size: int) -> int:
        idx = 0
        active_batch_size = local_batch_size

        while idx < len(records):
            chunk = records[idx : idx + active_batch_size]
            texts = [str(rec.get("text", "")) for rec in chunk]

            while True:
                try:
                    scores = scorer.score_texts(texts)
                    for rec, score in zip(chunk, scores):
                        out_record = {
                            "text": rec.get("text", ""),
                            "label": rec.get("label"),
                            "perplexity_score": score,
                        }
                        out_handle.write(json.dumps(out_record, ensure_ascii=False) + "\n")
                    break
                except RuntimeError as err:
                    if _is_cuda_oom_error(err, scorer.device) and active_batch_size > 1:
                        active_batch_size = max(1, active_batch_size // 2)
                        chunk = records[idx : idx + active_batch_size]
                        texts = [str(rec.get("text", "")) for rec in chunk]
                        torch.cuda.empty_cache()
                        print(f"CUDA OOM: reducing batch_size to {active_batch_size} and retrying...")
                        continue
                    raise

            idx += len(chunk)

        return active_batch_size

    with input_path.open("r", encoding="utf-8") as src, output_path.open("w", encoding="utf-8") as dst:
        batch_records: list[dict[str, Any]] = []

        for line in src:
            raw_line = line.strip()
            if not raw_line:
                continue

            try:
                record = json.loads(raw_line)
            except json.JSONDecodeError:
                failed += 1
                continue

            batch_records.append(record)
            if len(batch_records) >= current_batch_size:
                current_batch_size = process_chunk(batch_records, dst, current_batch_size)
                processed += len(batch_records)
                batch_records.clear()

                if processed % 10000 == 0:
                    print(f"PPL progress: processed={processed}, failed={failed}, batch_size={current_batch_size}")

        if batch_records:
            current_batch_size = process_chunk(batch_records, dst, current_batch_size)
            processed += len(batch_records)

    return processed, failed


def build_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract text/label, deduplicate, and compute batched perplexity into JSONL outputs."
    )
    parser.add_argument("--model_name", type=str, default="vinai/PhoGPT-4B")
    parser.add_argument("--batch_size", type=int, default=16)
    parser.add_argument("--max_length", type=int, default=256)
    parser.add_argument("--force_cpu", action="store_true")
    return parser.parse_args()


def _resolve_path(preferred: Path) -> Path:
    if preferred.exists():
        return preferred

    alt_in_outputs = BASE_DIR / "outputs" / preferred.name
    if alt_in_outputs.exists():
        return alt_in_outputs

    # If no input file exists yet, keep the preferred path for writing outputs.
    return preferred


def main() -> None:
    args = build_args()

    jobs = [
        (_resolve_path(BASE_DIR / "output_method1.jsonl"), _resolve_path(BASE_DIR / "output_method1_text_label.jsonl")),
        (_resolve_path(BASE_DIR / "output_method2.jsonl"), _resolve_path(BASE_DIR / "output_method2_text_label.jsonl")),
    ]

    print(
        "Loading perplexity scorer "
        f"(model={args.model_name}, batch_size={args.batch_size}, max_length={args.max_length})"
    )
    scorer = BatchedPerplexityScorer(
        model_name=args.model_name,
        max_length=args.max_length,
        force_cpu=args.force_cpu,
    )

    for input_path, output_path in jobs:
        page_count, sentence_count = extract_text_label(input_path, output_path)
        print(
            f"Done: {input_path.name} -> {output_path.name} | pages={page_count}, sentences={sentence_count}"
        )

        dedup_output_path = output_path.with_name(f"{output_path.stem}_dedup.jsonl")
        total_in, unique_count = deduplicate_text_label(output_path, dedup_output_path)
        duplicate_count = total_in - unique_count
        print(
            f"Dedup: {output_path.name} -> {dedup_output_path.name} | "
            f"input={total_in}, unique={unique_count}, removed={duplicate_count}"
        )

        ppl_output_path = dedup_output_path.with_name(f"{dedup_output_path.stem}_ppl.jsonl")
        processed, failed = add_perplexity_scores(
            dedup_output_path,
            ppl_output_path,
            scorer=scorer,
            batch_size=args.batch_size,
        )
        print(
            f"PPL: {dedup_output_path.name} -> {ppl_output_path.name} | "
            f"processed={processed}, failed_parse={failed}"
        )


if __name__ == "__main__":
    main()