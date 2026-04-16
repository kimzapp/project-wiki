# Quick Reference: Code Changes Detail

## File: `05_extract_sentences/extract_paragraphs_and_sentences.py`

---

## 📝 Change 1: Added Unicode Import

```python
# BEFORE
import csv
import re
from underthesea import sent_tokenize
import json
import sys

# AFTER
import csv
import re
import unicodedata  # ✅ ADDED
from underthesea import sent_tokenize
import json
import sys
```

---

## 🔧 Change 2: Added `normalize_unicode()` Function

```python
# ✅ NEW FUNCTION (added at line ~165)
def normalize_unicode(text: str) -> str:
    """
    Chuẩn hoá unicode về dạng NFC 
    (Canonical Decomposition, followed by Canonical Composition)
    Đảm bảo nhất quán khi xử lý các ký tự unicode phức tạp
    """
    return unicodedata.normalize('NFC', text)
```

---

## 🔪 Change 3: Consolidated `remove_ref_tags()` Function

```python
# BEFORE (duplicate definition - removed old one)
def remove_tags_except_ref(text: str) -> str:
    if not text:
        return ""
    text = BLOCK_TAG_EXCEPT_REF.sub("", text)
    text = SELF_CLOSING_EXCEPT_REF.sub("", text)
    return text
    
# Duplicate definition of remove_ref_tags() - REMOVED ❌

# AFTER (single, consolidated definition with better docs)
def remove_ref_tags(text: str) -> str:
    """
    Loại bỏ <ref>...</ref> tags vì chúng không cần thiết cho việc extract text.
    Hữu ích sau khi loại bỏ các HTML tags khác.
    """
    if not text:
        return ""
    # Xoá <ref ...>...</ref>
    text = re.sub(
        r"<ref\b[^>]*>.*?</ref>",
        "",
        text,
        flags=re.DOTALL | re.IGNORECASE
    )
    # Xoá <ref .../> (self-closing)
    text = re.sub(
        r"<ref\b[^>]*/\s*>",
        "",
        text,
        flags=re.IGNORECASE
    )
    return text
```

---

## ❌ Change 4: Removed Labeling Functions

```python
# ❌ REMOVED - These functions no longer exist
def has_ref_tag(text: str) -> bool:
    return bool(re.search(r"<ref\b", text, flags=re.IGNORECASE))

def label_sentences(sentences):
    """
    Gán nhãn cho một câu, nếu trong câu có thẻ <ref> thì là positive,
    ngược lại là negative
    return list các tuple (sentence, label)
    """
    labels = []
    for sent in sentences:
        if has_ref_tag(sent):
            cleaned_sent = remove_ref_tags(sent)
            labels.append((cleaned_sent, 1))
        else:
            labels.append((sent, 0))
    return labels
```

---

## 📚 Change 5: Enhanced `extract_sentences()` Function

```python
# BEFORE
def extract_sentences(text):
    """
    Lấy ra các câu từ nội dung một paragraph
    """
    sentences = sent_tokenize(text)
    return sentences

# AFTER
def extract_sentences(text):
    """
    Lấy ra các câu từ nội dung một paragraph.
    Sử dụng underthesea để tokenize câu tiếng Việt.
    
    Args:
        text: đoạn văn dạng string
    
    Returns:
        list các câu đã được chuẩn hoá
    """
    if not text:  # ✅ Early return for empty text
        return []
    
    sentences = sent_tokenize(text)
    
    # ✅ NEW: Chuẩn hoá các câu
    cleaned_sentences = []
    for sent in sentences:
        sent = sent.strip()
        if sent:  # ✅ NEW: Bỏ qua các câu rỗng
            sent = normalize_unicode(sent)  # ✅ NEW: Unicode normalization
            sent = re.sub(r'\s+', ' ', sent)  # ✅ IMPROVED: Better whitespace
            cleaned_sentences.append(sent)
    
    return cleaned_sentences
```

---

## 🔄 Change 6: Completely Refactored `main()` Function

### Location: Main processing loop

```python
# ❌ REMOVED OLD CODE
for p_idx, p in enumerate(paragraphs):
    sentences = extract_sentences(p)
    labeled_sentences = label_sentences(sentences)  # ❌ REMOVED

    paragraph_obj = {
        "paragraph_id": p_idx,
        "text": p,
        "sentences": []
    }

    for s_idx, (sent, label) in enumerate(labeled_sentences):
        paragraph_obj["sentences"].append({
            "sentence_id": s_idx,  # ❌ REMOVED ID
            "text": sent,
            "label": label  # ❌ REMOVED LABEL
        })

# ✅ NEW CODE
for p_idx, p in enumerate(paragraphs):
    # ✅ NEW: Paragraph-level normalization
    p = normalize_unicode(p)
    p = re.sub(r'\s+', ' ', p)
    
    sentences = extract_sentences(p)

    if sentences:  # ✅ Check if sentences exist before adding
        paragraph_obj = {
            "paragraph_id": p_idx,
            "text": p,
            "sentences": sentences  # ✅ Simple list of strings (no labels/IDs)
        }
        page_obj["paragraphs"].append(paragraph_obj)
```

---

## 🎯 Change 7: Added Unicode & Ref Removal to Main Processing

```python
# BEFORE
for title, page_id, text in iter_pages_from_csv(csv_path):
    cleaned_text = remove_tags_except_ref(text)
    removed_templates = remove_templates(cleaned_text)
    removed_tables = remove_wikitables(removed_templates)
    paragraphs = extract_paragraphs(removed_tables)

# AFTER
for title, page_id, text in iter_pages_from_csv(csv_path):
    # ✅ NEW: Step 0 - Unicode normalization at input
    text = normalize_unicode(text)
    
    # Step 1: Remove HTML tags (except <ref>)
    cleaned_text = remove_tags_except_ref(text)
    
    # Step 2: Remove templates
    removed_templates = remove_templates(cleaned_text)
    
    # Step 3: Remove wikitables
    removed_tables = remove_wikitables(removed_templates)
    
    # ✅ NEW: Step 4 - Remove <ref> tags (was not done before!)
    removed_refs = remove_ref_tags(removed_tables)
    
    # Step 5: Extract paragraphs
    paragraphs = extract_paragraphs(removed_refs)
```

---

## 📊 Summary of Changes

| Type | Count | Details |
|------|-------|---------|
| **Functions Added** | 1 | `normalize_unicode()` |
| **Functions Removed** | 2 | `has_ref_tag()`, `label_sentences()` |
| **Functions Modified** | 2 | `extract_sentences()`, `main()` |
| **Imports Added** | 1 | `unicodedata` |
| **Output Structure** | Changed | Removed sentence-level IDs and labels |
| **Processing Steps** | Expanded | Added 2 new steps (unicode norm, ref removal) |

---

## ⚡ Key Behavioral Changes

1. **Sentence Extraction**: Now includes Unicode normalization and better whitespace handling
2. **Paragraph Processing**: Each paragraph is Unicode-normalized before sentence extraction
3. **Ref Tag Handling**: `<ref>` tags are completely removed in main pipeline (not preserved for labeling)
4. **Output Format**: Sentences are now plain strings instead of objects with ID and label
5. **Empty Sentence Filtering**: Empty sentences after tokenization are filtered out
6. **Text Normalization**: Multiple whitespace characters (including newlines) collapsed to single space

---

## ✔️ Verification

- **Syntax Check**: ✅ PASSED (No errors)
- **Import Validation**: ✅ All imports available
- **Function Changes**: ✅ All modifications verified
- **Logic Flow**: ✅ Processing pipeline confirmed

**Ready for testing on sample data!**
