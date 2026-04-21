import argparse
import shlex
import sys
from datetime import datetime
from pathlib import Path

import torch

try:
    import wandb
except ImportError:
    wandb = None

from text_cls.data import build_data_bundle
from text_cls.engines import run_rnn_training, run_transformer_training
from text_cls.models import build_rnn_model, build_transformer_model
from text_cls.utils import (
    count_trainable_params,
    ensure_dir,
    resolve_device,
    save_json,
    set_seed,
    write_run_summary,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Train text classifier with RNN or Transformer")

    parser.add_argument(
        "--data_path",
        type=str,
        default="/home/rmits/project-wiki/05_sentences/sampled_method1.jsonl",
    )
    parser.add_argument("--output_dir", type=str, default="/home/rmits/project-wiki/06_models/results_text_cls")

    parser.add_argument("--model_type", type=str, choices=["rnn", "transformer"], required=True)
    parser.add_argument("--model_name", type=str, default="vinai/phobert-base")

    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max_length", type=int, default=256)
    parser.add_argument("--batch_size", type=int, default=16)
    parser.add_argument("--num_workers", type=int, default=0)

    parser.add_argument("--num_epochs", type=int, default=3)
    parser.add_argument("--learning_rate", type=float, default=2e-5)
    parser.add_argument("--weight_decay", type=float, default=0.01)

    parser.add_argument("--logging_steps", type=int, default=50)
    parser.add_argument("--smoke_steps", type=int, default=None)
    parser.add_argument("--force_cpu", action="store_true")
    parser.add_argument("--fp16", action="store_true")
    parser.add_argument("--bf16", action="store_true")

    parser.add_argument("--rnn_type", type=str, choices=["lstm", "gru"], default="lstm")
    parser.add_argument("--embedding_dim", type=int, default=256)
    parser.add_argument("--hidden_size", type=int, default=256)
    parser.add_argument("--num_layers", type=int, default=1)
    parser.add_argument("--dropout", type=float, default=0.2)
    parser.add_argument("--bidirectional", action="store_true")
    parser.add_argument("--grad_clip_norm", type=float, default=1.0)
    parser.add_argument("--patience", type=int, default=2)

    parser.add_argument("--use_wandb", action="store_true")
    parser.add_argument("--wandb_project", type=str, default="project-wiki-text-cls")
    parser.add_argument("--wandb_entity", type=str, default=None)
    parser.add_argument("--wandb_run_name", type=str, default=None)
    parser.add_argument("--wandb_mode", type=str, choices=["online", "offline"], default="online")
    parser.add_argument("--wandb_tags", type=str, default="")
    parser.add_argument("--wandb_api_key", type=str, default=None)

    return parser.parse_args()


def build_reproduce_cli_command() -> str:
    script_path = Path(__file__).resolve()
    python_exec = shlex.quote(sys.executable)
    script = shlex.quote(str(script_path))
    arg_str = " ".join(shlex.quote(arg) for arg in sys.argv[1:])
    return f"{python_exec} {script}{(' ' + arg_str) if arg_str else ''}"


def build_experiment_config(
    args,
    out_dir: Path,
    run_name: str,
    device: str,
    gpu_count: int,
    multi_gpu_enabled: bool,
) -> dict:
    cli_params = vars(args).copy()

    common_hparams = {
        "seed": args.seed,
        "max_length": args.max_length,
        "batch_size": args.batch_size,
        "num_workers": args.num_workers,
        "num_epochs": args.num_epochs,
        "learning_rate": args.learning_rate,
        "weight_decay": args.weight_decay,
        "logging_steps": args.logging_steps,
        "smoke_steps": args.smoke_steps,
    }

    model_hparams = {}
    if args.model_type == "transformer":
        model_hparams = {
            "fp16": args.fp16,
            "bf16": args.bf16,
        }
    else:
        model_hparams = {
            "rnn_type": args.rnn_type,
            "embedding_dim": args.embedding_dim,
            "hidden_size": args.hidden_size,
            "num_layers": args.num_layers,
            "dropout": args.dropout,
            "bidirectional": args.bidirectional,
            "grad_clip_norm": args.grad_clip_norm,
            "patience": args.patience,
        }

    return {
        "experiment_name": run_name,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "output_dir": str(out_dir),
        "model": {
            "model_type": args.model_type,
            "model_name": args.model_name,
        },
        "runtime": {
            "device": device,
            "force_cpu": args.force_cpu,
            "gpu_count": gpu_count,
            "multi_gpu_enabled": multi_gpu_enabled,
        },
        "hyperparameters": {
            "common": common_hparams,
            "model_specific": model_hparams,
        },
        "cli": {
            "parameters": cli_params,
            "reproduce_command": build_reproduce_cli_command(),
        },
    }


def sanitize_model_name(model_name: str) -> str:
    model_name = model_name.strip().replace(" ", "_").replace("/", "_").lower()
    return model_name


def parse_wandb_tags(tags: str) -> list[str]:
    if not tags:
        return []
    return [tag.strip() for tag in tags.split(",") if tag.strip()]


def init_wandb_run(args, run_name: str, out_dir: Path, config: dict):
    if not args.use_wandb:
        return None

    if wandb is None:
        raise ImportError(
            "wandb is not installed in the active environment. "
            "Please install it with: pip install wandb"
        )

    wandb_run_name = args.wandb_run_name or run_name
    tags = parse_wandb_tags(args.wandb_tags)
    wandb.login(key=args.wandb_api_key)
    return wandb.init(
        project=args.wandb_project,
        entity=args.wandb_entity,
        name=wandb_run_name,
        dir=str(out_dir),
        config=config,
        tags=tags,
        mode=args.wandb_mode,
    )

def main():
    args = parse_args()
    set_seed(args.seed)

    run_name = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_root_dir = ensure_dir(Path(args.output_dir) / sanitize_model_name(args.model_name))
    out_dir = ensure_dir(model_root_dir / run_name)

    data_bundle = build_data_bundle(
        data_path=args.data_path,
        model_name=args.model_name,
        model_type=args.model_type,
        max_length=args.max_length,
        batch_size=args.batch_size,
        seed=args.seed,
        num_workers=args.num_workers,
    )

    device = resolve_device(force_cpu=args.force_cpu)
    gpu_count = torch.cuda.device_count() if (device.type == "cuda") else 0
    multi_gpu_enabled = gpu_count > 1

    experiment_config = build_experiment_config(
        args=args,
        out_dir=out_dir,
        run_name=run_name,
        device=str(device),
        gpu_count=gpu_count,
        multi_gpu_enabled=multi_gpu_enabled,
    )
    wandb_run = init_wandb_run(args=args, run_name=run_name, out_dir=out_dir, config=experiment_config)

    if device.type == "cuda":
        if multi_gpu_enabled:
            print(f"[info] Auto-detected {gpu_count} GPUs. Multi-GPU mode is enabled.")
        else:
            print("[info] Auto-detected 1 GPU. Running in single-GPU mode.")
    else:
        print("[info] Running on CPU.")

    try:
        if args.model_type == "transformer":
            model = build_transformer_model(
                model_name=args.model_name,
                num_labels=data_bundle.num_labels,
                label2id=data_bundle.label2id,
                id2label=data_bundle.id2label,
            )

            train_result, trainer = run_transformer_training(
                model=model,
                tokenizer=data_bundle.tokenizer,
                data_bundle=data_bundle,
                output_dir=str(out_dir),
                learning_rate=args.learning_rate,
                num_epochs=args.num_epochs,
                weight_decay=args.weight_decay,
                logging_steps=args.logging_steps,
                device=device,
                fp16=args.fp16,
                bf16=args.bf16,
                wandb_run=wandb_run,
            )

            best_model_dir = ensure_dir(out_dir / "best_model")
            trainer.save_model(str(best_model_dir))
            data_bundle.tokenizer.save_pretrained(str(best_model_dir))

        else:
            model = build_rnn_model(
                vocab_size=data_bundle.tokenizer.vocab_size,
                num_labels=data_bundle.num_labels,
                rnn_type=args.rnn_type,
                embedding_dim=args.embedding_dim,
                hidden_size=args.hidden_size,
                num_layers=args.num_layers,
                bidirectional=args.bidirectional,
                dropout=args.dropout,
                pad_token_id=data_bundle.tokenizer.pad_token_id,
            )

            train_result = run_rnn_training(
                model=model,
                data_bundle=data_bundle,
                output_dir=str(out_dir),
                learning_rate=args.learning_rate,
                num_epochs=args.num_epochs,
                weight_decay=args.weight_decay,
                grad_clip_norm=args.grad_clip_norm,
                patience=args.patience,
                smoke_steps=args.smoke_steps,
                device=device,
                wandb_run=wandb_run,
            )

            data_bundle.tokenizer.save_pretrained(str(ensure_dir(out_dir / "tokenizer")))

    finally:
        if wandb_run is not None:
            wandb_run.finish()

    metrics = {
        "best_val": train_result.best_val_metrics,
        "test": train_result.test_metrics,
        "num_labels": data_bundle.num_labels,
        "label2id": data_bundle.label2id,
        "id2label": data_bundle.id2label,
        "class_distribution": data_bundle.class_distribution,
        "model_type": args.model_type,
        "model_name": args.model_name,
        "device": str(device),
        "trainable_params": count_trainable_params(model),
    }

    save_json(out_dir / "metrics.json", metrics)
    save_json(out_dir / "experiment_config.json", experiment_config)
    write_run_summary(out_dir / "run_summary.txt", {
        "output_dir": str(out_dir),
        "model_root_dir": str(model_root_dir),
        "model_type": args.model_type,
        "model_name": args.model_name,
        "best_val_macro_f1": metrics["best_val"].get("macro_f1", "n/a"),
        "test_macro_f1": metrics["test"].get("macro_f1", "n/a"),
    })

    print(f"Saved artifacts to: {out_dir}")
    print(f"Best validation metrics: {metrics['best_val']}")
    print(f"Test metrics: {metrics['test']}")
    print(
        f"Runtime device: {device} | gpu_count={gpu_count} | multi_gpu_enabled={multi_gpu_enabled}"
    )


if __name__ == "__main__":
    main()
