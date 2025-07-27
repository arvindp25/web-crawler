import requests
import json
import random
import time
import os
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
from io import BytesIO
from sqlalchemy.exc import IntegrityError
from db.conn import SessionLocal 
from db.models import CrawlRecord
from urllib.parse import urlparse
import spacy
import re

CDX_INDEX = "https://index.commoncrawl.org/CC-MAIN-2025-13-index"
QUERY_SIZE = 1000
MAX_PAGES = 10
WARC_BASE = "https://data.commoncrawl.org/"
nlp = spacy.load("en_core_web_sm")


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
        print(f"[i] Extracted Page {page}...")
        print(f"[i] Waiting {delay:.2f}s before next page...")
        time.sleep(delay)

def normalize_company_name(title: str) -> str:
    """Extract cleaner company name from title string."""
    if not title:
        return None
    if "–" in title:
        return title.split("–")[-1].strip()
    if "-" in title:
        return title.split("-")[-1].strip()
    return title.strip()


def extract_company_name_from_html(soup: BeautifulSoup, html_text: str = "") -> str:
    # 1. Meta tags
    meta_tags = [
        ('meta', {'property': 'og:site_name'}),
        ('meta', {'name': 'og:site_name'}),
        ('meta', {'name': 'description'}),
        ('meta', {'property': 'og:title'})
    ]
    for tag in meta_tags:
        meta = soup.find(*tag)
        if meta and meta.get('content'):
            return normalize_company_name(meta['content'])

    # 2. Title tag
    if soup.title and soup.title.string:
        return normalize_company_name(soup.title.string)

    # 3. H1 fallback
    h1 = soup.find("h1")
    if h1:
        return normalize_company_name(h1.text)

    # 4. Footer pattern (e.g., © 2024 XYZ Pty Ltd)
    footer_text = soup.get_text()
    match = re.search(r'© ?\d{4} ?(.+?)(\.|All rights reserved|$)', footer_text, re.IGNORECASE)
    if match:
        return normalize_company_name(match.group(1))

    # 5. Named Entity Recognition (ORG)
    if html_text:
        doc = nlp(html_text)
        for ent in doc.ents:
            if ent.label_ == "ORG":
                return normalize_company_name(ent.text)

    return None


seen_domains = set()
def download_and_extract_company_data(
    entries: list[dict],
    digest_set: set
) -> list[dict]:
    """
    Downloads WARC byte ranges for selected entries, parses HTML for company name,
    and deduplicates based on digest + domain.
    Only processes the first occurrence per domain.
    """
    matched_records = []

    for entry in entries:
        if entry.get("digest") not in digest_set:
            continue

        target_url = entry.get("url")
        if not target_url:
            continue
        parsed = urlparse(target_url)
        domain = parsed.netloc.lower()

        # Strict: only allow one page per domain — first occurrence
        if domain in seen_domains:
            print(f"[i] Skipping domain already seen: {domain}")
            continue

        # Mark domain early
        seen_domains.add(domain)

        warc_path = entry["warc_path"]
        offset = int(entry["offset"])
        length = int(entry["length"])
        start = offset
        end = offset + length - 1
        warc_url = f"{WARC_BASE}{warc_path}"

        headers = {"Range": f"bytes={start}-{end}"}
        print(f"📦 Downloading WARC fragment: {warc_url} [{start}-{end}]")

        try:
            response = requests.get(warc_url, headers=headers, stream=True, timeout=30)
            response.raise_for_status()
        except Exception as e:
            print(f"[!] Failed to fetch WARC range: {e}")
            continue

        try:
            for record in ArchiveIterator(response.raw, arc2warc=True):
                if record.rec_type != "response":
                    continue

                payload = record.content_stream().read()
                soup = BeautifulSoup(payload, "html.parser")
                text_content = soup.get_text()

                # 🔍 Improved extraction
                text_content = soup.get_text()
                title_tag = soup.title.string.strip() if soup.title and soup.title.string else None
                clean_name = extract_company_name_from_html(soup, text_content)


                matched_records.append({
                    "url": target_url,
                    "company_name": clean_name,
                    "title": title_tag,
                    "text": text_content,
                    "digest": entry.get("digest"),
                    "timestamp": entry.get("timestamp")
                })

                break  # Only process one record

        except Exception as parse_err:
            print(f"[!] Error parsing WARC response: {parse_err}")

    return matched_records
