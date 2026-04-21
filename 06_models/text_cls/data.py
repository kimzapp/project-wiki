import json
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from numbers import Number
from pathlib import Path

import torch
from sklearn.model_selection import train_test_split
from torch.nn.utils.rnn import pad_sequence
from torch.utils.data import DataLoader, Dataset
from transformers import AutoTokenizer, DataCollatorWithPadding


@dataclass
class DataBundle:
    train_loader: DataLoader
    val_loader: DataLoader
    test_loader: DataLoader
    label2id: dict[str, int]
    id2label: dict[int, str]
    num_labels: int
    tokenizer: AutoTokenizer
    class_distribution: dict[str, int]


class TextDataset(Dataset):
    def __init__(self, texts, labels, tokenizer, max_length: int):
        self.texts = texts
        self.labels = labels
        self.tokenizer = tokenizer
        self.max_length = max_length

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        encoded = self.tokenizer(
            self.texts[idx],
            truncation=True,
            max_length=self.max_length,
            padding=False,
            return_attention_mask=True,
        )
        encoded["labels"] = int(self.labels[idx])
        return encoded


def _validate_dataset_samples(dataset: TextDataset, num_labels: int, max_checks: int = 1024) -> None:
    checks = min(len(dataset), max_checks)
    if checks == 0:
        raise ValueError("Dataset split is empty.")

    for idx in range(checks):
        sample = dataset[idx]

        input_ids = sample.get("input_ids")
        if not isinstance(input_ids, list) or len(input_ids) == 0:
            raise ValueError(f"Invalid sample at index {idx}: empty or missing input_ids.")

        labels = sample.get("labels")
        if not isinstance(labels, int):
            raise TypeError(f"Invalid label type at index {idx}: {type(labels)}")
        if labels < 0 or labels >= num_labels:
            raise ValueError(
                f"Label out of range at index {idx}: label={labels}, num_labels={num_labels}"
            )

        attention_mask = sample.get("attention_mask")
        if attention_mask is not None:
            if not isinstance(attention_mask, list) or len(attention_mask) != len(input_ids):
                raise ValueError(
                    f"Invalid attention_mask at index {idx}: expected list length {len(input_ids)}"
                )
            if any(x not in (0, 1) for x in attention_mask):
                raise ValueError(f"Invalid attention_mask values at index {idx}: must be 0/1")


def clean_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", str(text))
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _load_jsonl(data_path: str, text_key: str, label_key: str):
    texts, raw_labels = [], []
    dropped_empty = 0
    dropped_invalid = 0

    with open(data_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                dropped_invalid += 1
                continue

            text = obj.get(text_key)
            if text is None:
                dropped_invalid += 1
                continue

            text = clean_text(text)
            if not text:
                dropped_empty += 1
                continue

            label = obj.get(label_key)
            if label is None:
                dropped_invalid += 1
                continue

            texts.append(text)
            raw_labels.append(label)

    if not texts:
        raise ValueError(f"No valid records found in {data_path}")

    stats = {
        "num_records": len(texts),
        "dropped_empty_text": dropped_empty,
        "dropped_invalid": dropped_invalid,
    }
    return texts, raw_labels, stats


def _normalize_labels(raw_labels):
    if all(isinstance(x, Number) for x in raw_labels):
        classes = sorted(set(int(x) for x in raw_labels))
        label2id = {str(c): i for i, c in enumerate(classes)}
        labels = [label2id[str(int(lbl))] for lbl in raw_labels]
        id2label = {v: k for k, v in label2id.items()}
        return labels, label2id, id2label

    classes = sorted({str(x) for x in raw_labels})
    label2id = {c: i for i, c in enumerate(classes)}
    labels = [label2id[str(lbl)] for lbl in raw_labels]
    id2label = {v: k for k, v in label2id.items()}
    return labels, label2id, id2label


def _safe_train_test_split(texts, labels, test_size, seed, use_stratify: bool):
    stratify = labels if use_stratify else None
    try:
        return train_test_split(
            texts,
            labels,
            test_size=test_size,
            random_state=seed,
            stratify=stratify,
        )
    except ValueError:
        return train_test_split(
            texts,
            labels,
            test_size=test_size,
            random_state=seed,
            stratify=None,
        )


def _rnn_collate(batch, pad_token_id: int):
    input_ids = [torch.tensor(x["input_ids"], dtype=torch.long) for x in batch]
    labels = torch.tensor([x["labels"] for x in batch], dtype=torch.long)

    lengths = torch.tensor([len(x) for x in input_ids], dtype=torch.long)
    padded_ids = pad_sequence(input_ids, batch_first=True, padding_value=pad_token_id)

    attention_mask = (padded_ids != pad_token_id).long()
    return {
        "input_ids": padded_ids,
        "attention_mask": attention_mask,
        "lengths": lengths,
        "labels": labels,
    }


def build_data_bundle(
    data_path: str,
    model_name: str,
    model_type: str,
    max_length: int,
    batch_size: int,
    seed: int,
    text_key: str = "text",
    label_key: str = "label",
    num_workers: int = 0,
):
    data_path = str(Path(data_path).expanduser().resolve())
    texts, raw_labels, load_stats = _load_jsonl(data_path, text_key=text_key, label_key=label_key)

    labels, label2id, id2label = _normalize_labels(raw_labels)
    if len(label2id) < 2:
        raise ValueError("Need at least 2 classes for classification training.")

    train_texts, temp_texts, train_labels, temp_labels = _safe_train_test_split(
        texts,
        labels,
        test_size=0.2,
        seed=seed,
        use_stratify=True,
    )

    val_texts, test_texts, val_labels, test_labels = _safe_train_test_split(
        temp_texts,
        temp_labels,
        test_size=0.5,
        seed=seed,
        use_stratify=True,
    )

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    if tokenizer.pad_token_id is None:
        vocab = tokenizer.get_vocab() if hasattr(tokenizer, "get_vocab") else {}
        if "<pad>" in vocab:
            tokenizer.pad_token = "<pad>"
        elif "[PAD]" in vocab:
            tokenizer.pad_token = "[PAD]"
        elif tokenizer.eos_token is not None:
            tokenizer.pad_token = tokenizer.eos_token
        else:
            tokenizer.add_special_tokens({"pad_token": "[PAD]"})

    train_dataset = TextDataset(train_texts, train_labels, tokenizer, max_length)
    val_dataset = TextDataset(val_texts, val_labels, tokenizer, max_length)
    test_dataset = TextDataset(test_texts, test_labels, tokenizer, max_length)

    _validate_dataset_samples(train_dataset, num_labels=len(label2id))
    _validate_dataset_samples(val_dataset, num_labels=len(label2id))
    _validate_dataset_samples(test_dataset, num_labels=len(label2id))

    if model_type == "transformer":
        collate_fn = DataCollatorWithPadding(
            tokenizer=tokenizer,
            padding=True,
            return_tensors="pt",
        )
    else:
        collate_fn = lambda batch: _rnn_collate(batch, pad_token_id=tokenizer.pad_token_id)

    loader_kwargs = {
        "batch_size": batch_size,
        "num_workers": num_workers,
        "pin_memory": torch.cuda.is_available(),
        "collate_fn": collate_fn,
    }

    train_loader = DataLoader(train_dataset, pin_memory=True, shuffle=True, **loader_kwargs)
    val_loader = DataLoader(val_dataset, pin_memory=True, shuffle=False, **loader_kwargs)
    test_loader = DataLoader(test_dataset, pin_memory=True, shuffle=False, **loader_kwargs)

    class_distribution = Counter(labels)
    class_distribution = {
        id2label[k]: int(v) for k, v in sorted(class_distribution.items(), key=lambda x: x[0])
    }
    class_distribution.update(load_stats)

    return DataBundle(
        train_loader=train_loader,
        val_loader=val_loader,
        test_loader=test_loader,
        label2id=label2id,
        id2label=id2label,
        num_labels=len(label2id),
        tokenizer=tokenizer,
        class_distribution=class_distribution,
    )
