import torch
import math
from transformers import AutoTokenizer, AutoModelForCausalLM

# Model Vietnamese GPT
model_name = "vinai/PhoGPT-4B"   # nếu máy yếu có thể đổi sang model nhỏ hơn nếu có

tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForCausalLM.from_pretrained(
    model_name,
    torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32
)

device = "cuda" if torch.cuda.is_available() else "cpu"
model = model.to(device)
model.eval()


def compute_perplexity(text):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=256)
    input_ids = inputs["input_ids"].to(device)
    attn_masks = inputs['attention_mask'].to(device)

    with torch.no_grad():
        outputs = model(input_ids, labels=input_ids, attention_mask=attn_masks)
        loss = outputs.loss

    ppl = math.exp(loss.item())
    return ppl

sentences = [
    "Hôm nay trời rất đẹp.",
    "Tôi ăn cơm với gia đình.",
    "asdf qwer zxcv"  # câu vô nghĩa
]

for sent in sentences:
    ppl = compute_perplexity(sent)
    print(f"Text: {sent}")
    print(f"Perplexity: {ppl:.4f}")
    print("-" * 40)