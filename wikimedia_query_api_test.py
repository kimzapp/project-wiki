import requests
import time

API_URL = "https://vi.wikipedia.org/w/api.php"

HEADERS = {
    "User-Agent": (
        "RevisionCrawler/1.0 "
        "(Academic research; contact: your_email@example.com)"
    )
}


def iter_revision_history_with_text(
    title: str,
    sleep=0.2,
):
    """
    Crawl toàn bộ revision history của 1 page, kèm nội dung text
    (MediaWiki wikitext, không phải plain text)
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    params = {
        "action": "query",
        "prop": "revisions",
        "titles": title,
        "rvprop": "ids|timestamp|user|comment|content",
        "rvslots": "main",
        "rvlimit": 500,
        "rvdir": "newer",
        "format": "json",
        "formatversion": 2,
    }

    rvcontinue = None

    while True:
        if rvcontinue:
            params["rvcontinue"] = rvcontinue

        resp = session.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        pages = data["query"]["pages"]
        if not pages:
            break

        page = pages[0]
        page_id = page.get("pageid")
        page_title = page.get("title")

        for rev in page.get("revisions", []):
            slots = rev.get("slots", {})
            main = slots.get("main", {})
            text = main.get("content", "")

            yield {
                "pageid": page_id,
                "title": page_title,
                "revid": rev.get("revid"),
                "parentid": rev.get("parentid"),
                "timestamp": rev.get("timestamp"),
                "user": rev.get("user"),
                "comment": rev.get("comment"),
                "text": text,  # WIKITEXT
            }

        cont = data.get("continue")
        if not cont or "rvcontinue" not in cont:
            break

        rvcontinue = cont["rvcontinue"]
        time.sleep(sleep)


if __name__ == "__main__":
    import json

    with open("Lolei_revisions_with_text.jsonl", "w", encoding="utf-8") as f:
        for i, rev in enumerate(iter_revision_history_with_text("Lolei")):
            f.write(json.dumps(rev, ensure_ascii=False) + "\n")

        for i, rev in enumerate(iter_revision_history_with_text("Tiếng Lombard")):
            f.write(json.dumps(rev, ensure_ascii=False) + "\n")

        for i, rev in enumerate(iter_revision_history_with_text("Ulysses S. Grant")):
            f.write(json.dumps(rev, ensure_ascii=False) + "\n")
