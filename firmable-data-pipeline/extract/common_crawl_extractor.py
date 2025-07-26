import requests
import json
import random
import time
import os
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
from io import BytesIO
from sqlalchemy.exc import IntegrityError
from db.conn import SessionLocal  # adjust import as per your structure
from db.common_crawl_models import CrawlRecord  # adjust import as per your structure


CDX_INDEX = "https://index.commoncrawl.org/CC-MAIN-2025-13-index"
QUERY_SIZE = 1000
MAX_PAGES = 10
WARC_BASE = "https://data.commoncrawl.org/"


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
                            "industry": record.get("industry"),
                            "filename": record.get("filename"),
                            "offset": record.get("offset"),
                            "length": record.get("length")
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

def download_and_extract_company_data(
    entries: list[dict],
    digest_set: set
) -> list[dict]:
    """
    Given a list of WARC metadata entries (with offset, length, filename), 
    this function fetches only the necessary byte ranges, extracts HTML content, 
    and parses company name and naive industry metadata.
    """
    import requests
    from warcio.archiveiterator import ArchiveIterator
    from bs4 import BeautifulSoup

    matched_records = []

    for entry in entries:
        if entry.get("digest") not in digest_set:
            continue

        warc_path = entry["warc_path"]
        offset = int(entry["offset"])
        length = int(entry["length"])
        start = offset
        end = offset + length - 1
        url = f"{WARC_BASE}{warc_path}"

        headers = {"Range": f"bytes={start}-{end}"}
        print(f"📦 Downloading WARC fragment: {url} [{start}-{end}]")

        try:
            response = requests.get(url, headers=headers, stream=True, timeout=30)
            response.raise_for_status()
        except Exception as e:
            print(f"[!] Failed to fetch range: {e}")
            continue

        try:
            for record in ArchiveIterator(response.raw, arc2warc=True):
                if record.rec_type != "response":
                    continue

                payload = record.content_stream().read()
                soup = BeautifulSoup(payload, "html.parser")

                title = soup.title.string.strip() if soup.title and soup.title.string else None

                # Naive industry detection from meta
                meta_industry = None
                for meta in soup.find_all("meta"):
                    if meta.get("name", "").lower() in {"keywords", "description"}:
                        meta_industry = meta.get("content", "").strip().lower()
                        break

                matched_records.append({
                    "url": record.rec_headers.get("WARC-Target-URI"),
                    "company_name": title,
                    "industry": meta_industry,
                    "digest": entry.get("digest")
                })

        except Exception as parse_err:
            print(f"[!] Error parsing WARC response: {parse_err}")

    return matched_records
