import requests
import time
import sys


def download_bot_list(
    wiki="en",
    output_file="bot_list.txt",
    sleep_time=0.5,
    max_retries=3,
):
    """
    Download official bot user list from Wikipedia using MediaWiki API.

    Args:
        wiki (str): language code, e.g. 'en', 'vi', 'de'
        output_file (str): file to save bot usernames
        sleep_time (float): delay between paginated requests
        max_retries (int): retry attempts if request fails
    """

    URL = f"https://{wiki}.wikipedia.org/w/api.php"

    session = requests.Session()
    session.headers.update({
        "User-Agent": "BotListDownloader/1.0 (research purpose; contact: your_email@example.com)"
    })

    params = {
        "action": "query",
        "list": "allusers",
        "augroup": "bot",
        "aulimit": "max",
        "format": "json"
    }

    bot_users = set()

    while True:
        for attempt in range(max_retries):
            try:
                response = session.get(URL, params=params, timeout=15)

                if response.status_code != 200:
                    print(f"[WARNING] HTTP {response.status_code}")
                    time.sleep(2)
                    continue

                try:
                    data = response.json()
                except ValueError:
                    print("[ERROR] Response is not valid JSON")
                    print("First 500 chars:")
                    print(response.text[:500])
                    return

                break  # request successful

            except requests.exceptions.RequestException as e:
                print(f"[ERROR] Request failed: {e}")
                time.sleep(2)
        else:
            print("[FATAL] Max retries exceeded.")
            return

        # Extract bot users
        users = data.get("query", {}).get("allusers", [])
        for user in users:
            bot_users.add(user["name"])

        print(f"Collected so far: {len(bot_users)} bots")

        # Handle pagination
        if "continue" in data:
            params.update(data["continue"])
            time.sleep(sleep_time)
        else:
            break

    # Save to file
    with open(output_file, "w", encoding="utf-8") as f:
        for username in sorted(bot_users):
            f.write(username + "\n")

    print(f"\n✅ Finished. Total bots: {len(bot_users)}")
    print(f"Saved to: {output_file}")


if __name__ == "__main__":
    # Change 'en' to 'vi' if needed
    download_bot_list(wiki="vi", output_file="bot_list_vi.txt")