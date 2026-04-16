import csv
import re
import unicodedata
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

REF_TAG_RE = re.compile(
    r"<ref\b[^>]*?>.*?</ref\s*>|<ref\b[^>]*/\s*>",
    flags=re.DOTALL | re.IGNORECASE,
)

NUMERIC_CITATION_RE = re.compile(r"\[(\d+)\]")
TRAILING_NUMERIC_CITATION_RE = re.compile(
    r"^\s*(?:[.!?…]+\s*)?(?:\[\d+\]\s*)+(?:[.!?…]+\s*)?"
)

def remove_tags_except_ref(text: str) -> str:
    if not text:
        return ""

    text = BLOCK_TAG_EXCEPT_REF.sub("", text)
    text = SELF_CLOSING_EXCEPT_REF.sub("", text)
    return text


def normalize_unicode(text: str) -> str:
    if not text:
        return ""
    return unicodedata.normalize("NFC", text)


def remove_invisible_chars(text: str) -> str:
    if not text:
        return ""
    # Loại bỏ các ký tự zero-width và BOM dễ gây lệch tách câu.
    return re.sub(r"[\u200B-\u200D\uFEFF\u2060]", "", text)


def remove_html_comments(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)


def normalize_inline_whitespace(text: str) -> str:
    if not text:
        return ""
    # Không đụng vào '\n' để tránh phá ranh giới paragraph.
    text = text.replace("\u00A0", " ")
    return re.sub(r"[ \t\r\f\v]+", " ", text).strip()


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


def strip_ref_tags_with_positions(text: str):
    """
    Trả về:
      - cleaned_text: text đã bỏ <ref>
      - ref_positions: vị trí (trên cleaned_text) nơi ref từng xuất hiện
    """
    if not text:
        return "", []

    cleaned_parts = []
    ref_positions = []
    last_end = 0
    cleaned_len = 0

    for match in REF_TAG_RE.finditer(text):
        left = text[last_end:match.start()]
        cleaned_parts.append(left)
        cleaned_len += len(left)
        ref_positions.append(cleaned_len)
        last_end = match.end()

    tail = text[last_end:]
    cleaned_parts.append(tail)

    return "".join(cleaned_parts), ref_positions

# ========================================
#   EXTRACT PARAGRAPHS
# ========================================

# section pattern
SECTION_RE = re.compile(
    r'(={2,6}\s*[^=\n]+\s*={2,6})'
)

SECTION_LINE_RE = re.compile(r'^\s*(={2,6})\s*([^=\n]+?)\s*\1\s*$')
SECTION_PREFIX_RE = re.compile(r'^\s*(={2,6})\s*([^=\n]+?)\s*\1\s*')

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


def parse_section_header(text: str):
    """
    Trả về (header, remaining_text).
    - Nếu text là một dòng heading: remaining_text rỗng.
    - Nếu text bắt đầu bằng heading: cắt heading khỏi đầu đoạn.
    - Nếu không có heading: header = None.
    """
    if not text:
        return None, ""

    full_heading_match = SECTION_LINE_RE.match(text)
    if full_heading_match:
        header = full_heading_match.group(2).strip()
        return header, ""

    prefix_heading_match = SECTION_PREFIX_RE.match(text)
    if prefix_heading_match:
        header = prefix_heading_match.group(2).strip()
        remaining = text[prefix_heading_match.end():].strip()
        return header, remaining

    return None, text


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
    current_header = None
    for p in text.split("\n\n"):
        p = p.strip()

        header, remaining_text = parse_section_header(p)
        if header is not None:
            current_header = header
            if not remaining_text:
                continue
            p = remaining_text

        # bỏ heading, list, bảng
        if (
            not p
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

        paragraphs.append({
            "header": current_header,
            "text": p,
        })

    return paragraphs

# ========================================
#   EXTRACT SENTENCES
# ========================================

def extract_sentences(text):
    """
    Lấy ra các câu từ nội dung một paragraph
    """

    if not text:
        return []

    sentences = sent_tokenize(text)
    cleaned_sentences = []
    for sent in sentences:
        sent = sent.strip()
        if sent:
            cleaned_sentences.append(sent)
    return cleaned_sentences


def has_ref_between_positions(ref_positions, start_pos: int, end_pos: int) -> bool:
    """
    Một ref thuộc về câu nếu vị trí ref nằm trong [start_pos, end_pos].
    end_pos là exclusive trong Python, nhưng ta cho phép ref ở đúng cuối câu
    (ví dụ: "... .<ref>...</ref>").
    """
    for pos in ref_positions:
        if start_pos <= pos <= end_pos:
            return True
    return False


def has_numeric_citation_for_sentence(cleaned_paragraph: str, start_pos: int, end_pos: int) -> bool:
    """
    Xem một câu có citation dạng [1], [12], [1][2] hay không.
    Bao gồm cả trường hợp citation nằm trong câu hoặc ngay sau câu,
    trước/sau dấu kết thúc câu.
    """
    if start_pos < 0 or end_pos < 0 or end_pos <= start_pos:
        return False

    sentence_text = cleaned_paragraph[start_pos:end_pos]
    if NUMERIC_CITATION_RE.search(sentence_text):
        return True

    right_context = cleaned_paragraph[end_pos:end_pos + 32]
    return bool(TRAILING_NUMERIC_CITATION_RE.match(right_context))


def map_sentence_citations_strict(raw_paragraph: str, cleaned_sentences):
    """
    Strict mapping: tìm tuần tự từng câu cleaned trong raw-paragraph-sau-khi-bỏ-ref.
    Nếu không tìm thấy khớp chính xác thì gán False.
    """
    cleaned_paragraph, ref_positions = strip_ref_tags_with_positions(raw_paragraph)

    flags = []
    cursor = 0

    for sent in cleaned_sentences:
        start = cleaned_paragraph.find(sent, cursor)
        if start < 0:
            flags.append(False)
            continue

        end = start + len(sent)
        has_ref_tag = has_ref_between_positions(ref_positions, start, end)
        has_numeric_ref = has_numeric_citation_for_sentence(cleaned_paragraph, start, end)
        flags.append(has_ref_tag or has_numeric_ref)
        cursor = end

    return flags

def remove_ref_tags(text: str) -> str:
    cleaned_text, _ = strip_ref_tags_with_positions(text)
    return cleaned_text

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
            text = normalize_unicode(text)
            text = remove_invisible_chars(text)
            text = remove_html_comments(text)

            cleaned_text = remove_tags_except_ref(text)
            removed_templates = remove_templates(cleaned_text)
            removed_tables = remove_wikitables(removed_templates)
            paragraphs = extract_paragraphs(removed_tables)

            page_obj = {
                "page_id": page_id,
                "title": title,
                "paragraphs": []
            }

            for p_idx, paragraph in enumerate(paragraphs):
                paragraph_header = paragraph.get("header")
                p = paragraph.get("text") or ""

                paragraph_raw = normalize_unicode(p)
                paragraph_raw = remove_invisible_chars(paragraph_raw)
                paragraph_raw = normalize_inline_whitespace(paragraph_raw)

                paragraph_clean = remove_ref_tags(paragraph_raw)

                sentences = extract_sentences(paragraph_clean)
                citation_flags = map_sentence_citations_strict(paragraph_raw, sentences)

                paragraph_obj = {
                    "paragraph_id": p_idx,
                    "header": paragraph_header,
                    "text_raw": paragraph_raw,
                    "text_clean": paragraph_clean,
                    "sentences": []
                }

                for s_idx, sent in enumerate(sentences):
                    has_citation = citation_flags[s_idx] if s_idx < len(citation_flags) else False
                    paragraph_obj["sentences"].append({
                        "index": s_idx,
                        "text": normalize_inline_whitespace(sent),
                        "has_citation": bool(has_citation)
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