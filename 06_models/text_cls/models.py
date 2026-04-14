import torch
import torch.nn as nn
from transformers import AutoModelForSequenceClassification


class RNNClassifier(nn.Module):
    def __init__(
        self,
        vocab_size: int,
        num_labels: int,
        rnn_type: str = "lstm",
        embedding_dim: int = 256,
        hidden_size: int = 256,
        num_layers: int = 1,
        bidirectional: bool = True,
        dropout: float = 0.2,
        pad_token_id: int = 1,
    ):
        super().__init__()
        self.pad_token_id = pad_token_id
        self.embedding = nn.Embedding(
            num_embeddings=vocab_size,
            embedding_dim=embedding_dim,
            padding_idx=pad_token_id,
        )
        self.embed_dropout = nn.Dropout(dropout)

        rnn_dropout = dropout if num_layers > 1 else 0.0
        rnn_cls = nn.LSTM if rnn_type.lower() == "lstm" else nn.GRU
        self.rnn = rnn_cls(
            input_size=embedding_dim,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=rnn_dropout,
            bidirectional=bidirectional,
        )

        out_size = hidden_size * (2 if bidirectional else 1)
        self.classifier = nn.Sequential(
            nn.Dropout(dropout),
            nn.Linear(out_size, num_labels),
        )
        self.loss_fn = nn.CrossEntropyLoss()
        self.rnn_type = rnn_type.lower()
        self.bidirectional = bidirectional

    def _take_last_hidden(self, hidden):
        if self.rnn_type == "lstm":
            hidden = hidden[0]

        if self.bidirectional:
            # Shape: (num_layers * 2, batch, hidden_size)
            return torch.cat([hidden[-2], hidden[-1]], dim=1)
        return hidden[-1]

    def forward(self, input_ids, attention_mask=None, lengths=None, labels=None):
        emb = self.embed_dropout(self.embedding(input_ids))
        if lengths is None:
            lengths = (input_ids != self.pad_token_id).sum(dim=1)

        packed = nn.utils.rnn.pack_padded_sequence(
            emb,
            lengths.cpu(),
            batch_first=True,
            enforce_sorted=False,
        )
        _, hidden = self.rnn(packed)
        feat = self._take_last_hidden(hidden)
        logits = self.classifier(feat)

        out = {"logits": logits}
        if labels is not None:
            loss = self.loss_fn(logits, labels)
            # Keep loss gather-safe under multi-GPU DataParallel.
            if loss.dim() == 0:
                loss = loss.unsqueeze(0)
            out["loss"] = loss
        return out


def build_transformer_model(
    model_name: str,
    num_labels: int,
    label2id: dict[str, int],
    id2label: dict[int, str],
):
    return AutoModelForSequenceClassification.from_pretrained(
        model_name,
        num_labels=num_labels,
        label2id=label2id,
        id2label={str(k): v for k, v in id2label.items()},
    )


def build_rnn_model(
    vocab_size: int,
    num_labels: int,
    rnn_type: str,
    embedding_dim: int,
    hidden_size: int,
    num_layers: int,
    bidirectional: bool,
    dropout: float,
    pad_token_id: int,
):
    if rnn_type.lower() not in {"lstm", "gru"}:
        raise ValueError("rnn_type must be one of: lstm, gru")

    return RNNClassifier(
        vocab_size=vocab_size,
        num_labels=num_labels,
        rnn_type=rnn_type,
        embedding_dim=embedding_dim,
        hidden_size=hidden_size,
        num_layers=num_layers,
        bidirectional=bidirectional,
        dropout=dropout,
        pad_token_id=pad_token_id,
    )
