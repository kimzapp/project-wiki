# =====================================================
#                  DOMAIN / CLEANER
# =====================================================

import re
import mwparserfromhell
from gensim.corpora.wikicorpus import filter_wiki


def normalize_text(text: str) -> str:
    if not text:
        return ""

    text = filter_wiki(text).strip()

    wikicode = mwparserfromhell.parse(text)
    for tpl in wikicode.filter_templates(recursive=True):
        wikicode.remove(tpl)

    clean = wikicode.strip_code()
    clean = re.sub(r"\s+", " ", clean).strip()

    return clean


def count_citations(raw_text: str) -> int:
    if not raw_text:
        return 0

    # Match full ref blocks and self-closing refs
    pattern = re.compile(
        r"<ref\b([^>/]*?)>(.*?)</ref>|<ref\b([^>/]*?)/>",
        re.IGNORECASE | re.DOTALL
    )

    seen_names = set()
    count = 0

    for match in pattern.finditer(raw_text):
        attrs = match.group(1) or match.group(3) or ""

        name_match = re.search(r'name\s*=\s*["\']?([^"\'>\s]+)', attrs, re.IGNORECASE)

        if name_match:
            name = name_match.group(1)
            if name not in seen_names:
                seen_names.add(name)
                count += 1
        else:
            count += 1

    return count
