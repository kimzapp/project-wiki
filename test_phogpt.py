# coding: utf8
import torch
from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer

model_path = "vinai/PhoGPT-4B"  

config = AutoConfig.from_pretrained(model_path, trust_remote_code=True)  
config.init_device = "cuda"
# config.attn_config['attn_impl'] = 'flash' # If installed: this will use either Flash Attention V1 or V2 depending on what is installed

model = AutoModelForCausalLM.from_pretrained(model_path, config=config, torch_dtype=torch.bfloat16, trust_remote_code=True)
# If your GPU does not support bfloat16:
# model = AutoModelForCausalLM.from_pretrained(model_path, config=config, torch_dtype=torch.float16, trust_remote_code=True)
model.eval()  

tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True)  

import torch
import math

def compute_perplexity(text, model, tokenizer):
    encodings = tokenizer(text, return_tensors="pt")
    input_ids = encodings.input_ids.to(model.device)

    with torch.no_grad():
        outputs = model(input_ids, labels=input_ids)
        loss = outputs.loss

    if torch.isnan(loss):
        return float("inf")

    return math.exp(loss.item())

text = "Guyana là nước ký sau cùng vào ngày 20 tháng 10 năm 2008 tại Brussels."
ppl = compute_perplexity(text, model, tokenizer)
print(ppl)