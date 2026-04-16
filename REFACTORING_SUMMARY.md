# Extract Paragraphs & Sentences - Refactoring Summary

**Date:** April 15, 2026  
**File:** `05_extract_sentences/extract_paragraphs_and_sentences.py`

---

## ✅ Refactoring Completed

### 1. Removed Sentence-Level Label Inference ❌➡️📄

**Deleted Functions:**
- `has_ref_tag()` - No longer needed
- `label_sentences()` - Main labeling function (REMOVED)
- Duplicate `remove_ref_tags()` definition (consolidated)

**Before (Old)**
```python
def main():
    for title, page_id, text in iter_pages_from_csv(csv_path):
        paragraphs = extract_paragraphs(removed_tables)
        
        for p_idx, p in enumerate(paragraphs):
            sentences = extract_sentences(p)
            labeled_sentences = label_sentences(sentences)  # ❌ REMOVED
            
            for s_idx, (sent, label) in enumerate(labeled_sentences):
                paragraph_obj["sentences"].append({
                    "sentence_id": s_idx,
                    "text": sent,
                    "label": label  # Binary: 0 or 1
                })
```

**After (New)**
```python
def main():
    for title, page_id, text in iter_pages_from_csv(csv_path):
        paragraphs = extract_paragraphs(removed_refs)  # Now includes ref removal
        
        for p_idx, p in enumerate(paragraphs):
            sentences = extract_sentences(p)  # No labeling
            
            if sentences:
                paragraph_obj = {
                    "paragraph_id": p_idx,
                    "text": p,
                    "sentences": sentences  # Simple list of strings
                }
```

---

### 2. Enhanced Text Cleaning & Normalization 🔧

#### Added Unicode Normalization (NFC)
```python
import unicodedata  # ✅ NEW

def normalize_unicode(text: str) -> str:
    """
    Chuẩn hoá unicode về dạng NFC 
    (Canonical Decomposition, followed by Canonical Composition)
    """
    return unicodedata.normalize('NFC', text)
```

**Applied at Multiple Stages:**
1. **Raw text input** - Before any HTML tag removal
2. **Paragraph level** - Before sentence extraction
3. **Sentence level** - Within `extract_sentences()` function

#### Improved Whitespace Normalization
```python
# Before: re.sub(r'[ \t]+', ' ', text)  # Only spaces/tabs
# After:  re.sub(r'\s+', ' ', text)     # All whitespace (including newlines)
```

#### Consolidated `<ref>` Tag Removal
- **Now moved to main processing pipeline** (Step 4)
- Properly integrated into text cleaning stages
- Prevents `<ref>` tags from appearing in final output

---

### 3. Output Structure Comparison 📊

#### BEFORE (With Sentence Labels)
```json
{
  "page_id": "123",
  "title": "Article Title",
  "paragraphs": [
    {
      "paragraph_id": 0,
      "text": "This is a paragraph...",
      "sentences": [
        {
          "sentence_id": 0,
          "text": "First sentence.",
          "label": 1
        },
        {
          "sentence_id": 1,
          "text": "Second sentence.",
          "label": 0
        }
      ]
    }
  ]
}
```

#### AFTER (Simple Extraction)
```json
{
  "page_id": "123",
  "title": "Article Title",
  "paragraphs": [
    {
      "paragraph_id": 0,
      "text": "This is a paragraph...",
      "sentences": [
        "First sentence.",
        "Second sentence."
      ]
    }
  ]
}
```

---

### 4. Processing Pipeline Flow 🔄

#### New Preprocessing Steps (in Main Function)
```
Raw Text
    ↓
1. Unicode Normalization (NFC)
    ↓
2. Remove HTML tags (except <ref>)
    ↓
3. Remove templates (infobox, navbox, etc.)
    ↓
4. Remove wikitables
    ↓
5. Remove <ref> tags ✅ NEW STEP
    ↓
6. Extract paragraphs
    ↓
7. For each paragraph:
   - Unicode normalization
   - Whitespace normalization
   - Extract sentences (with cleaning)
    ↓
Output (JSONL format)
```

---

### 5. Improved `extract_sentences()` Function 📝

```python
def extract_sentences(text):
    """
    Lấy ra các câu từ nội dung một paragraph.
    Sử dụng underthesea để tokenize câu tiếng Việt.
    
    Returns:
        list các câu đã được chuẩn hoá
    """
    if not text:
        return []
    
    sentences = sent_tokenize(text)
    
    # Chuẩn hoá các câu
    cleaned_sentences = []
    for sent in sentences:
        sent = sent.strip()
        if sent:  # Bỏ qua các câu rỗng ✅ NEW
            sent = normalize_unicode(sent)  # ✅ NEW
            sent = re.sub(r'\s+', ' ', sent)  # ✅ IMPROVED
            cleaned_sentences.append(sent)
    
    return cleaned_sentences
```

**Improvements:**
- ✅ Early return for empty/None text
- ✅ Filters empty sentences after tokenization
- ✅ Unicode normalization per sentence
- ✅ Better whitespace handling
- ✅ More robust error handling

---

### 6. Key Improvements Summary

| Aspect | Before | After |
|--------|--------|-------|
| **Labeling** | Binary labels (0/1) per sentence | ❌ No labels - extract only |
| **Unicode Handling** | Minimal | ✅ NFC normalization 3x (raw, para, sent) |
| **Whitespace** | Partial cleanup | ✅ Comprehensive normalization |
| **<ref> Tags** | Preserved for labeling | ✅ Removed during cleanup |
| **Empty Sentences** | Included | ✅ Filtered out |
| **Output Complexity** | Complex structure | ✅ Simplified format |
| **Maintainability** | High code duplication | ✅ Cleaner architecture |

---

### 7. Backwards Compatibility ⚠️

**Breaking Changes:**
- Output format changed (no `sentence_id` and `label` fields)
- Removal of `label_sentences()` function
- Removal of `has_ref_tag()` function
- Changed processing order (ref tag removal now in main pipeline)

**Migration Path:**
If downstream code expects the old format with labels:
- Either recreate labels from a separate classifier
- Or update downstream code to work with simple sentence strings

---

### 8. Testing Recommendations

Before deploying, test on sample data:

```bash
python extract_paragraphs_and_sentences.py \
  --csv-path /path/to/sample.csv \
  --output-path /path/to/test_output.jsonl
```

**Verify:**
1. ✅ Output is valid JSONL format
2. ✅ No `<ref>` tags in output sentences
3. ✅ Unicode characters (Vietnamese) are properly normalized
4. ✅ Multiple consecutive spaces are collapsed
5. ✅ Sentences are properly extracted
6. ✅ Output structure matches new format

---

## Summary

The refactoring successfully:
- 🗑️ **Removed** sentence-level label inference (labeling logic)
- 🧹 **Improved** text cleaning with Unicode normalization (NFC)
- 📦 **Simplified** output structure (extracted only, no labels)
- 🚀 **Enhanced** robustness in sentence extraction
- 📝 **Maintained** code quality and readability

The script now focuses purely on **extracting paragraphs and sentences**, leaving classification for downstream modules.
