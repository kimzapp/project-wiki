# =====================================================
#               INFRASTRUCTURE / IO
# =====================================================

import bz2

def open_bz2_stream(path: str):
    return bz2.open(path, mode="rt", encoding="utf-8", errors="ignore")


def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1]
