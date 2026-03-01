# =====================================================
#                     DOMAIN
# =====================================================

from dataclasses import dataclass
from typing import Optional


@dataclass
class RevisionRecord:
    page_id: int
    page_title: str
    revision_id: int
    parent_revision_id: Optional[int]
    timestamp: str
    user_id: Optional[int]
    username: Optional[str]
    is_anonymous: bool
    is_bot: bool
    raw_text_len: int
    clean_text_len: int
    clean_text: str
