from __future__ import annotations

import copy
from dataclasses import dataclass

import numpy as np
import torch
from sklearn.metrics import accuracy_score, f1_score
from torch.optim import AdamW
from transformers import Trainer, TrainingArguments

from .utils import maybe_limit_batches


@dataclass
class TrainResult:
    best_val_metrics: dict
    test_metrics: dict


def _unwrap_model(model: torch.nn.Module) -> torch.nn.Module:
    if isinstance(model, torch.nn.DataParallel):
        return model.module
    return model


def compute_metrics_np(labels: np.ndarray, preds: np.ndarray) -> dict:
    return {
        "accuracy": float(accuracy_score(labels, preds)),
        "macro_f1": float(f1_score(labels, preds, average="macro", zero_division=0)),
    }


def run_transformer_training(
    model,
    tokenizer,
    data_bundle,
    output_dir: str,
    learning_rate: float,
    num_epochs: int,
    weight_decay: float,
    logging_steps: int,
    device: torch.device,
    fp16: bool = False,
    bf16: bool = False,
    wandb_run=None,
):
    if device.type == "cuda":
        gpu_count = torch.cuda.device_count()
        if gpu_count > 1:
            print(f"[info] Detected {gpu_count} GPUs. Hugging Face Trainer will use multi-GPU automatically.")
        elif gpu_count == 1:
            print("[info] Detected 1 GPU. Training on single GPU.")

    def _compute_metrics(eval_pred):
        logits, labels = eval_pred
        preds = np.argmax(logits, axis=1)
        return compute_metrics_np(labels, preds)

    def _validate_transformer_batch() -> None:
        batch = next(iter(data_bundle.train_loader), None)
        if batch is None:
            raise ValueError("Training loader returned no batches.")

        input_ids = batch.get("input_ids")
        labels = batch.get("labels")
        attention_mask = batch.get("attention_mask")

        if not torch.is_tensor(input_ids):
            raise TypeError(f"input_ids must be a tensor, got {type(input_ids)}")
        if input_ids.numel() == 0 or input_ids.dim() != 2:
            raise ValueError(
                f"input_ids must be 2D and non-empty, got shape={tuple(input_ids.shape)}"
            )

        if not torch.is_tensor(labels):
            raise TypeError(f"labels must be a tensor, got {type(labels)}")
        if labels.dtype != torch.long:
            raise TypeError(f"labels dtype must be torch.long, got {labels.dtype}")
        if labels.numel() == 0:
            raise ValueError("labels tensor is empty.")

        num_labels = int(model.config.num_labels)
        min_label = int(labels.min().item())
        max_label = int(labels.max().item())
        if min_label < 0 or max_label >= num_labels:
            raise ValueError(
                "Label out of range: "
                f"min={min_label}, max={max_label}, expected=[0, {num_labels - 1}]"
            )

        if attention_mask is not None:
            if not torch.is_tensor(attention_mask):
                raise TypeError(f"attention_mask must be a tensor, got {type(attention_mask)}")
            if attention_mask.shape != input_ids.shape:
                raise ValueError(
                    "attention_mask shape mismatch: "
                    f"mask={tuple(attention_mask.shape)}, input_ids={tuple(input_ids.shape)}"
                )

            if not torch.all((attention_mask == 0) | (attention_mask == 1)):
                raise ValueError("attention_mask must contain only 0/1 values.")
            if torch.any(attention_mask.sum(dim=1) == 0):
                raise ValueError("Detected empty sample in batch (all-zero attention_mask row).")

        print(
            "[sanity] batch_ok "
            f"input_shape={tuple(input_ids.shape)} "
            f"labels_dtype={labels.dtype} "
            f"label_range=[{min_label}, {max_label}] "
            f"pad_token_id={tokenizer.pad_token_id}"
        )

    _validate_transformer_batch()

    use_cpu = str(device) == "cpu"

    if use_cpu and (fp16 or bf16):
        print("[info] CPU mode detected: disabling fp16/bf16 for transformer training.")
        fp16 = False
        bf16 = False

    if (not torch.cuda.is_available()) and fp16:
        print("[info] CUDA is unavailable: disabling fp16.")
        fp16 = False

    if (not torch.cuda.is_available()) and bf16:
        print("[info] CUDA is unavailable: disabling bf16.")
        bf16 = False

    if fp16 and bf16:
        raise ValueError("fp16 and bf16 cannot both be enabled.")

    args = TrainingArguments(
        output_dir=output_dir,
        learning_rate=learning_rate,
        per_device_train_batch_size=data_bundle.train_loader.batch_size,
        per_device_eval_batch_size=data_bundle.val_loader.batch_size,
        num_train_epochs=num_epochs,
        eval_strategy="epoch",
        save_strategy="epoch",
        logging_strategy="steps",
        logging_steps=logging_steps,
        load_best_model_at_end=True,
        metric_for_best_model="macro_f1",
        greater_is_better=True,
        save_total_limit=1,
        weight_decay=weight_decay,
        report_to="wandb" if wandb_run is not None else "none",
        run_name=wandb_run.name if wandb_run is not None else None,
        use_cpu=use_cpu,
        fp16=fp16,
        bf16=bf16,
    )

    trainer = Trainer(
        model=model,
        args=args,
        train_dataset=data_bundle.train_loader.dataset,
        eval_dataset=data_bundle.val_loader.dataset,
        processing_class=tokenizer,
        data_collator=data_bundle.train_loader.collate_fn,
        compute_metrics=_compute_metrics,
    )

    trainer.train()

    val_metrics = trainer.evaluate(eval_dataset=data_bundle.val_loader.dataset)
    test_metrics = trainer.evaluate(eval_dataset=data_bundle.test_loader.dataset)

    if wandb_run is not None:
        wandb_run.log({
            "final/val_loss": val_metrics.get("eval_loss"),
            "final/val_accuracy": val_metrics.get("eval_accuracy"),
            "final/val_macro_f1": val_metrics.get("eval_macro_f1"),
            "final/test_loss": test_metrics.get("eval_loss"),
            "final/test_accuracy": test_metrics.get("eval_accuracy"),
            "final/test_macro_f1": test_metrics.get("eval_macro_f1"),
        })

    return TrainResult(best_val_metrics=val_metrics, test_metrics=test_metrics), trainer


def _move_batch_to_device(batch: dict, device: torch.device) -> dict:
    return {
        key: value.to(device) if torch.is_tensor(value) else value
        for key, value in batch.items()
    }


def _run_rnn_epoch(
    model,
    loader,
    device: torch.device,
    optimizer=None,
    grad_clip_norm: float | None = None,
    smoke_steps: int | None = None,
):
    is_train = optimizer is not None
    model.train(is_train)

    losses = []
    all_preds = []
    all_labels = []

    for step_idx, batch in enumerate(loader):
        batch = _move_batch_to_device(batch, device)

        out = model(
            input_ids=batch["input_ids"],
            attention_mask=batch.get("attention_mask"),
            lengths=batch.get("lengths"),
            labels=batch["labels"],
        )
        loss = out["loss"]
        if torch.is_tensor(loss) and loss.dim() > 0:
            loss = loss.mean()

        if is_train:
            optimizer.zero_grad(set_to_none=True)
            loss.backward()
            if grad_clip_norm is not None and grad_clip_norm > 0:
                torch.nn.utils.clip_grad_norm_(model.parameters(), grad_clip_norm)
            optimizer.step()

        losses.append(loss.detach().item())
        preds = torch.argmax(out["logits"], dim=1)
        preds_cpu = preds.detach().cpu()
        labels_cpu = batch["labels"].detach().cpu()
        if preds_cpu.dim() == 0:
            preds_cpu = preds_cpu.unsqueeze(0)
        if labels_cpu.dim() == 0:
            labels_cpu = labels_cpu.unsqueeze(0)

        all_preds.append(preds_cpu)
        all_labels.append(labels_cpu)

        if maybe_limit_batches(step_idx, smoke_steps):
            break

    labels_np = torch.cat(all_labels).numpy()
    preds_np = torch.cat(all_preds).numpy()
    metrics = compute_metrics_np(labels_np, preds_np)
    metrics["loss"] = float(np.mean(losses)) if losses else 0.0
    return metrics


def run_rnn_training(
    model,
    data_bundle,
    output_dir: str,
    learning_rate: float,
    num_epochs: int,
    weight_decay: float,
    grad_clip_norm: float,
    patience: int,
    smoke_steps: int | None,
    device: torch.device,
    wandb_run=None,
):
    model.to(device)

    if device.type == "cuda":
        gpu_count = torch.cuda.device_count()
        if gpu_count > 1:
            print(f"[info] Detected {gpu_count} GPUs. Enabling torch.nn.DataParallel for RNN training.")
            model = torch.nn.DataParallel(model)
        elif gpu_count == 1:
            print("[info] Detected 1 GPU. Training on single GPU.")

    optimizer = AdamW(model.parameters(), lr=learning_rate, weight_decay=weight_decay)

    best_state = None
    best_val_f1 = -1.0
    best_val_metrics = {}
    epochs_no_improve = 0

    for epoch_idx in range(num_epochs):
        train_metrics = _run_rnn_epoch(
            model,
            data_bundle.train_loader,
            device,
            optimizer=optimizer,
            grad_clip_norm=grad_clip_norm,
            smoke_steps=smoke_steps,
        )

        val_metrics = _run_rnn_epoch(
            model,
            data_bundle.val_loader,
            device,
            optimizer=None,
            smoke_steps=smoke_steps,
        )

        if wandb_run is not None:
            wandb_run.log({
                "epoch": epoch_idx + 1,
                "train/loss": train_metrics["loss"],
                "train/accuracy": train_metrics["accuracy"],
                "train/macro_f1": train_metrics["macro_f1"],
                "val/loss": val_metrics["loss"],
                "val/accuracy": val_metrics["accuracy"],
                "val/macro_f1": val_metrics["macro_f1"],
            })

        if val_metrics["macro_f1"] > best_val_f1:
            best_val_f1 = val_metrics["macro_f1"]
            best_val_metrics = val_metrics
            best_state = copy.deepcopy(model.state_dict())
            epochs_no_improve = 0
        else:
            epochs_no_improve += 1
            if epochs_no_improve >= patience:
                break

    if best_state is not None:
        model.load_state_dict(best_state)

    test_metrics = _run_rnn_epoch(
        model,
        data_bundle.test_loader,
        device,
        optimizer=None,
        smoke_steps=smoke_steps,
    )

    if wandb_run is not None:
        wandb_run.log({
            "final/best_val_loss": best_val_metrics.get("loss"),
            "final/best_val_accuracy": best_val_metrics.get("accuracy"),
            "final/best_val_macro_f1": best_val_metrics.get("macro_f1"),
            "final/test_loss": test_metrics.get("loss"),
            "final/test_accuracy": test_metrics.get("accuracy"),
            "final/test_macro_f1": test_metrics.get("macro_f1"),
        })

    ckpt_path = f"{output_dir}/best_model.pt"
    torch.save(_unwrap_model(model).state_dict(), ckpt_path)

    return TrainResult(best_val_metrics=best_val_metrics, test_metrics=test_metrics)
