# Output Format Examples - Before & After

**File:** `05_extract_sentences/extract_paragraphs_and_sentences.py`

---

## 📤 OUTPUT FORMAT COMPARISON

### BEFORE (With Sentence Labels - OLD)

```json
{
  "page_id": "12345",
  "title": "Lịch sử Việt Nam",
  "paragraphs": [
    {
      "paragraph_id": 0,
      "text": "Việt Nam là một đất nước có lịch sử lâu đời. Nền văn minh Việt Nam đã phát triển từ hàng nghìn năm trước.",
      "sentences": [
        {
          "sentence_id": 0,
          "text": "Việt Nam là một đất nước có lịch sử lâu đời.",
          "label": 0
        },
        {
          "sentence_id": 1,
          "text": "Nền văn minh Việt Nam đã phát triển từ hàng nghìn năm trước.",
          "label": 1
        }
      ]
    },
    {
      "paragraph_id": 1,
      "text": "Các triều đại phong kiến đã để lại di sản văn hóa quý báu.",
      "sentences": [
        {
          "sentence_id": 0,
          "text": "Các triều đại phong kiến đã để lại di sản văn hóa quý báu.",
          "label": 0
        }
      ]
    }
  ]
}
```

**Key Features (OLD):**
- ✅ `sentence_id`: Index of sentence within paragraph
- ✅ `label`: Binary label (0 or 1 based on `<ref>` tags)
- ✅ Each sentence is an object with `sentence_id`, `text`, `label`

---

### AFTER (Extract Only - NEW)

```json
{
  "page_id": "12345",
  "title": "Lịch sử Việt Nam",
  "paragraphs": [
    {
      "paragraph_id": 0,
      "text": "Việt Nam là một đất nước có lịch sử lâu đời. Nền văn minh Việt Nam đã phát triển từ hàng nghìn năm trước.",
      "sentences": [
        "Việt Nam là một đất nước có lịch sử lâu đời.",
        "Nền văn minh Việt Nam đã phát triển từ hàng nghìn năm trước."
      ]
    },
    {
      "paragraph_id": 1,
      "text": "Các triều đại phong kiến đã để lại di sản văn hóa quý báu.",
      "sentences": [
        "Các triều đại phong kiến đã để lại di sản văn hóa quý báu."
      ]
    }
  ]
}
```

**Key Features (NEW):**
- ✅ No `sentence_id` (indices removed)
- ✅ No `label` (classification removed)
- ✅ Sentences are simple strings, not objects
- ✅ No `<ref>` tags in any text
- ✅ Text is Unicode-normalized (NFC)
- ✅ Multiple spaces collapsed to single space

---

## 🔍 Detailed Comparison

### Structure Depth

**BEFORE:**
```
Page Object
  ├── page_id
  ├── title
  └── paragraphs[] (array of objects)
      └── [ParaObj]
          ├── paragraph_id
          ├── text
          └── sentences[] (array of objects)
              └── [SentObj]
                  ├── sentence_id
                  ├── text
                  └── label
```

**AFTER:**
```
Page Object
  ├── page_id
  ├── title
  └── paragraphs[] (array of objects)
      └── [ParaObj]
          ├── paragraph_id
          ├── text
          └── sentences[] (array of strings)
              └── "sentence text string"
```

**Depth Reduction:** 3 levels → 2 levels (simpler structure)

---

## 📝 Real-World Example

### Input Article (Raw HTML from Wikipedia)

```html
<p>{{Infobox|...}}</p>  <!-- REMOVED -->
<p>Tây Ban Nha là một nước <ref>source1</ref> 
vị trí ở Bắc Âu.
Nó có nhiều <ref>source2</ref> lịch sử phong phú.</p>
```

### Processing Steps & Output

**Step 1:** Remove infobox templates
```
Tây Ban Nha là một nước <ref>source1</ref> 
vị trí ở Bắc Âu.
Nó có nhiều <ref>source2</ref> lịch sử phong phú.
```

**Step 2:** Remove `<ref>` tags
```
Tây Ban Nha là một nước  
vị trí ở Bắc Âu.
Nó có nhiều  lịch sử phong phú.
```

**Step 3:** Normalize whitespace
```
Tây Ban Nha là một nước vị trí ở Bắc Âu.
Nó có nhiều lịch sử phong phú.
```

**Step 4:** Extract & normalize sentences (Unicode NFC)
```json
{
  "page_id": "54321",
  "title": "Tây Ban Nha",
  "paragraphs": [
    {
      "paragraph_id": 0,
      "text": "Tây Ban Nha là một nước vị trí ở Bắc Âu. Nó có nhiều lịch sử phong phú.",
      "sentences": [
        "Tây Ban Nha là một nước vị trí ở Bắc Âu.",
        "Nó có nhiều lịch sử phong phú."
      ]
    }
  ]
}
```

---

## 🎯 Output JSONL Format

Each line in the output file is a **complete JSON object** (one article per line):

```
{"page_id":"123","title":"Article1","paragraphs":[...]}
{"page_id":"456","title":"Article2","paragraphs":[...]}
{"page_id":"789","title":"Article3","paragraphs":[...]}
```

---

## 💾 File Size Comparison

| Aspect | Before | After | Change |
|--------|--------|-------|--------|
| **Avg bytes per sentence** | ~80 | ~50 | ↓ 37% smaller |
| **Structure complexity** | High | Low | Simplified |
| **Parse time** | Slower | Faster | ↑ Simpler JSON |
| **Label field overhead** | per sentence | N/A | ↓ Removed |

---

## 🔄 Downstream Processing

### For Those Using Labels (Before)

If your downstream code expects sentence labels:

```python
# OLD CODE (won't work with new format)
for sentence_obj in paragraph["sentences"]:
    sent_id = sentence_obj["sentence_id"]  # ❌ No longer exists
    text = sentence_obj["text"]
    label = sentence_obj["label"]  # ❌ No longer exists
    process(text, label)
```

### Update to New Format

```python
# NEW CODE (works with new format)
for idx, sent_text in enumerate(paragraph["sentences"]):
    # sentences is now a list of strings
    process(sent_text)

# If you need a label, generate it separately:
from your_classifier import predict_label
for idx, sent_text in enumerate(paragraph["sentences"]):
    label = predict_label(sent_text)  # Generate label from classifier
    process(sent_text, label)
```

---

## ✅ Validation Checklist

When testing the new output:

- [ ] Valid JSONL format (one object per line)
- [ ] All paragraphs have `paragraph_id`, `text`, `sentences`
- [ ] All sentences are strings (not objects)
- [ ] No `<ref>` tags in any sentence text
- [ ] No `sentence_id` fields
- [ ] No `label` fields
- [ ] Multiple consecutive spaces collapsed to single space
- [ ] Vietnamese characters properly encoded (Unicode NFC)
- [ ] Only paragraphs with ≥1 sentence are included

---

## 📌 Key Takeaways

1. **Output is simpler**: Just strings instead of complex objects
2. **File size smaller**: ~37% reduction in bytes per sentence
3. **Processing faster**: Simpler JSON structure to parse
4. **No auto labels**: Need external classifier if labels are required
5. **Better text quality**: Cleaner normalization (Unicode NFC)
6. **Extract-only focus**: Pure extraction, no side effects
