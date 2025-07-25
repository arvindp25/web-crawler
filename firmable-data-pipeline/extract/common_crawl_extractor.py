import requests
import json
import random
import time

CDX_INDEX = "https://index.commoncrawl.org/CC-MAIN-2025-13-index"
QUERY_SIZE = 1000
MAX_PAGES = 10


def search_common_crawl(domain_keyword: str, pages: int = MAX_PAGES):
    for page in range(pages):
        backoff = 1.0
        max_retries = 5
        retries = 0

        while retries < max_retries:
            try:
                params = {
                    "url": f"*.{domain_keyword}/*",
                    "output": "json",
                    "page": page,
                    "filter": "status:200",
                    "limit": QUERY_SIZE
                }
                url = f"{CDX_INDEX}?{requests.compat.urlencode(params)}"
                print(url)
                response = requests.get(url, timeout=10)

                if response.status_code == 200:
                    lines = response.text.strip().split("\n")
                    for line in lines:
                        record = json.loads(line)
                        yield {
                            "url": record.get("url"),
                            "timestamp": record.get("timestamp"),
                            "digest": record.get("digest"),
                            "mime": record.get("mime"),
                            "status": record.get("status"),
                            "industry": record.get("industry")
                        }
                    break  # success, move to next page
                elif response.status_code in (429, 503):
                    print(f"[!] Rate limit hit (HTTP {response.status_code}), backing off...")
                    time.sleep(backoff)
                    backoff *= 1.5
                    retries += 1
                else:
                    print(f"[!] Unexpected status {response.status_code}, skipping page.")
                    break
            except requests.RequestException as e:
                print(f"[!] Error: {e}, backing off...")
                time.sleep(backoff)
                backoff *= 1.5
                retries += 1

        # Always wait a random interval before next page
        delay = random.uniform(0.5, 2.0)
        print(f"[i] Waiting {delay:.2f}s before next page...")
        time.sleep(delay)
