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
