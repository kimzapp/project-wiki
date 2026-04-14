import csv
import re
from underthesea import sent_tokenize
import json
import sys
from argparse import ArgumentParser

csv.field_size_limit(sys.maxsize)

# ===================================
#   DUYỆT QUA TỪNG PAGE TỪ FILE CSV
#   return title, page_id, raw_text
# ===================================

def iter_pages_from_csv(csv_path):
    """
    Generator duyệt từng dòng article trong file CSV.
    Kỳ vọng cấu trúc cột: page_id, title, ..., raw_text
    Trả về: (title, page_id, raw_text)
    """
    with open(csv_path, mode="r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        required_cols = {"page_id", "title", "raw_text"}
        missing_cols = required_cols - set(reader.fieldnames or [])
        if missing_cols:
            raise ValueError(
                f"Thiếu cột bắt buộc trong CSV: {sorted(missing_cols)}. "
                "Cần ít nhất: page_id, title, raw_text"
            )

        for row in reader:
            title = (row.get("title") or "").strip()
            page_id = (row.get("page_id") or "").strip()
            text = row.get("raw_text") or ""

            # Bỏ qua dòng không có nội dung text.
            if not text.strip():
                continue

            yield title, page_id, text


# =========================================
#  LOẠI BỎ NỘI DUNG HTML, CHỈ GIỮ CITATION
# =========================================

# match <tag>...</tag> nhưng KHÔNG phải <ref>
BLOCK_TAG_EXCEPT_REF = re.compile(
    r"<(?!ref\b)([a-zA-Z0-9_:.-]+)(\s[^>]*)?>.*?</\1>",
    flags=re.DOTALL | re.IGNORECASE
)

# match self-closing tag nhưng KHÔNG phải <ref />
SELF_CLOSING_EXCEPT_REF = re.compile(
    r"<(?!ref\b)[^/>]+?/>",
    flags=re.DOTALL | re.IGNORECASE
)

def remove_tags_except_ref(text: str) -> str:
    if not text:
        return ""

    text = BLOCK_TAG_EXCEPT_REF.sub("", text)
    text = SELF_CLOSING_EXCEPT_REF.sub("", text)
    return text


# ==========================================
#  XOÁ CÁC TEMPLATE NHƯ INFOBOX, NAVBOX,...
# ==========================================

def remove_templates(text: str) -> str:
    """
    Xoá bỏ các template như infobox, navbox,... được gói trong các thẻ {{...}}
    """
    result = []
    i = 0
    depth = 0

    while i < len(text):
        if text.startswith("{{", i):
            depth += 1
            i += 2
        elif text.startswith("}}", i) and depth > 0:
            depth -= 1
            i += 2
        else:
            if depth == 0:
                result.append(text[i])
            i += 1

    return "".join(result)


def remove_wikitables(text):
    """
    Loại bỏ các cấu trúc bảng, được gói trong các thẻ {|...|}
    """
    result = []
    i = 0
    depth = 0

    while i < len(text):
        # Bắt đầu table
        if text.startswith('{|', i):
            depth += 1
            i += 2
            continue

        # Kết thúc table
        if depth > 0 and text.startswith('|}', i):
            depth -= 1
            i += 2
            continue

        # Chỉ giữ nội dung khi không nằm trong table
        if depth == 0:
            result.append(text[i])

        i += 1

    return ''.join(result)

# ========================================
#   EXTRACT PARAGRAPHS
# ========================================

# section pattern
SECTION_RE = re.compile(
    r'(={2,6}\s*[^=\n]+\s*={2,6})'
)

# category pattern
CAT_PATTERN = r'\[\[Thể loại:.*?\]\]'

def normalize_section_breaks(text):
    """
    Đảm bảo section header luôn được bao bởi \n\n
    """
    return SECTION_RE.sub(r'\n\n\1\n\n', text)


def extract_file_blocks(text):
    """
    Tách file/image blocks [[Tập tin:...]] hoặc [[File:...]]
    Trả về:
      - cleaned_text: text đã loại file
      - files: list các file block
    """
    i = 0
    depth = 0
    buffer = []
    files = []
    current = []

    while i < len(text):
        if text.startswith('[[', i):
            # kiểm tra có phải file không
            if depth == 0 and (
                text.startswith('[[Tập tin:', i) or
                text.startswith('[[Tập_tin:', i) or
                text.startswith('[[File:', i)
            ):
                depth = 1
                current = ['[[']
                i += 2
                continue
            elif depth > 0:
                depth += 1
                current.append('[[')
                i += 2
                continue

        if text.startswith(']]', i) and depth > 0:
            depth -= 1
            current.append(']]')
            i += 2
            if depth == 0:
                files.append(''.join(current))
                current = []
            continue

        if depth > 0:
            current.append(text[i])
        else:
            buffer.append(text[i])

        i += 1

    return ''.join(buffer), files


INTERNAL_LINK_RE = re.compile(r'\[\[([^\]]+?)\]\]')

def normalize_internal_links(text):
    def repl(match):
        content = match.group(1)
        # tách theo | và lấy phần cuối
        return content.split('|')[-1].strip()

    return INTERNAL_LINK_RE.sub(repl, text)


def extract_paragraphs(text, min_len=50):
    """
    Lấy ra các đoạn văn từ nội dung một article
    """

    # Chuẩn hoá lại các section breaks, đảm bảo tiêu đề được bao bởi \n\n
    text = normalize_section_breaks(text)

    # Đảm bảo các tập tin tách biệt với paragraph
    text, _ = extract_file_blocks(text)

    # Chuẩn hoá whitespace
    text = re.sub(r'\n{2,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)

    # Tách paragraph
    paragraphs = []
    for p in text.split("\n\n"):
        p = p.strip()

        # bỏ heading, list, bảng
        if (
            not p
            or p.startswith("=")
            or p.startswith("*")
            or p.startswith("#")
            or p.startswith("|")
            or p.startswith('<') # loại bỏ nốt các html tag còn sót lại
            or re.search(CAT_PATTERN, p) # loại bỏ đoạn category
            or len(p) < min_len
        ):
            continue

        # chuẩn hoá text
        ## loại bỏ các annotation ''', ''
        p = p.replace("'''", "")
        p = p.replace("''", "")

        # loại bỏ kí hiệu internal link
        p = normalize_internal_links(p)

        paragraphs.append(p)

    return paragraphs

# ========================================
#   EXTRACT SENTENCES
# ========================================

def extract_sentences(text):
    """
    Lấy ra các câu từ nội dung một paragraph
    """

    sentences = sent_tokenize(text)
    return sentences

def has_ref_tag(text: str) -> bool:
    return bool(re.search(r"<ref\b", text, flags=re.IGNORECASE))

def remove_ref_tags(text: str) -> str:
    # xoá <ref ...>...</ref>
    text = re.sub(
        r"<ref\b[^>]*>.*?</ref>",
        "",
        text,
        flags=re.DOTALL | re.IGNORECASE
    )
    # xoá <ref .../>
    text = re.sub(
        r"<ref\b[^>]*/\s*>",
        "",
        text,
        flags=re.IGNORECASE
    )
    return text

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

def argparse():
    parser = ArgumentParser(description="Extract sentences from Wikipedia articles.")
    parser.add_argument(
        "--csv-path",
        type=str,
        required=True,
        help="Path to the input CSV file containing articles.",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        required=True,
        help="Path to the output JSONL file for extracted sentences.",
        default='output_method1.jsonl'
    )
    return parser.parse_args()

def main():
    args = argparse()
    csv_path = args.csv_path
    output_path = args.output_path

    with open(output_path, 'w', encoding='utf-8') as fout:
        for title, page_id, text in iter_pages_from_csv(csv_path):
            cleaned_text = remove_tags_except_ref(text)
            removed_templates = remove_templates(cleaned_text)
            removed_tables = remove_wikitables(removed_templates)
            paragraphs = extract_paragraphs(removed_tables)

            page_obj = {
                "page_id": page_id,
                "title": title,
                "paragraphs": []
            }

            for p_idx, p in enumerate(paragraphs):
                sentences = extract_sentences(p)
                labeled_sentences = label_sentences(sentences)

                paragraph_obj = {
                    "paragraph_id": p_idx,
                    "text": p,
                    "sentences": []
                }

                for s_idx, (sent, label) in enumerate(labeled_sentences):
                    paragraph_obj["sentences"].append({
                        "sentence_id": s_idx,
                        "text": sent,
                        "label": label
                    })

                # chỉ thêm paragraph nếu có ít nhất 1 câu hợp lệ
                if paragraph_obj["sentences"]:
                    page_obj["paragraphs"].append(paragraph_obj)

            # chỉ ghi page nếu còn nội dung
            if page_obj["paragraphs"]:
                fout.write(json.dumps(page_obj, ensure_ascii=False) + "\n")
                print(f"✅ Xử lí thành công page id {page_id}: {title}")
            else:
                print(f"❌ Không có nội dung hợp lệ page id {page_id}: {title}")


if __name__ == "__main__":
    main()
