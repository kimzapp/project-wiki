# phobert
python train_text_cls.py \
  --model_type transformer \
  --model_name vinai/phobert-base \
  --data_path /home/rmits/project-wiki/05_sentences/sampled_method1.jsonl \
  --num_epochs 10 \
  --batch_size 32 \
  --use_wandb \
  --wandb_project project-wiki-data-auditing \
  --wandb_run_name phobert_exp1 \
  --wandb_tags transformer,phobert,method1

# rnn 
python train_text_cls.py \
  --model_type rnn \
  --rnn_type lstm \
  --model_name vinai/phobert-base \
  --data_path /home/rmits/project-wiki/05_sentences/sampled_method1.jsonl \
  --num_epochs 5 \
  --batch_size 32 \
  --bidirectional \
  --use_wandb \
  --wandb_project project-wiki-data-auditing \
  --wandb_run_name rnn_lstm_exp1 \
  --wandb_tags rnn,lstm,method1

# Output structure after each run:
#   /home/rmits/project-wiki/06_models/results_text_cls/<model_name>/<YYYYMMDD_HHMMSS>/
# Inside each experiment folder:
#   - experiment_config.json (CLI params + hyperparameters + reproduce command)
#   - metrics.json
#   - run_summary.txt