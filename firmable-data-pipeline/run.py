import threading
import argparse
import time
from db.conn import engine
from db.base import Base
from extract.abr_extractor import extract_abr_records, download_and_extract_abr_zip
from extract.common_crawl_extractor import search_common_crawl, download_and_extract_company_data
from load.loader import load_abr_records, load_crawl_records
from collections import defaultdict
from matcher.em import perform_string_matching
import os
import subprocess
BATCH_SIZE = 500


def run_abr_pipeline(abr_limit=3, record_limit=None):
    print("\n📥 Ensuring ABR XML files exist...")
    xml_paths = download_and_extract_abr_zip()
    print(f"  ✔ Found {len(xml_paths)} XML files.")
    if abr_limit:
        xml_paths = xml_paths[:abr_limit]

    print("🔄 Parsing and loading ABR records...")
    batch = []
    count = 0
    for record in extract_abr_records(xml_paths):
        batch.append(record)
        if len(batch) >= BATCH_SIZE:
            load_abr_records(batch)
            count += len(batch)
            print(f"  ✔ Loaded {count} ABR records so far...")
            batch = []
        if record_limit and count >= record_limit:
            break
    if batch and (not record_limit or count < record_limit):
        load_abr_records(batch)
        count += len(batch)
        print(f"  ✔ Loaded total {count} ABR records.")
    if count == 0:
        print("⚠️ No ABR records were loaded. Check XML contents or parsing logic.")




def run_common_crawl_pipeline(crawl_pages=100):
    print("\n🌍 Fetching Common Crawl index metadata...")
    domain = "com.au"
    records = list(search_common_crawl(domain, pages=crawl_pages))
    print(f"  ✔ Fetched {len(records)} crawl index records")

    # Group digests and metadata by WARC file
    warc_index = defaultdict(list)
    for rec in records:
        print(rec)
        warc_path = rec.get("filename")
        digest = rec.get("digest")
        offset = rec.get("offset")
        length = rec.get("length")
        timestamp = rec.get("timestamp")
        url = rec.get("url")
        if warc_path and digest and offset and length:
            warc_index[warc_path].append({
                "digest": digest,
                "offset": int(offset),
                "length": int(length),
                "warc_path": warc_path,
                "url": url,
                "timestamp": timestamp
            })

    print(f"🔁 Found {len(warc_index)} WARC files with matching digests")

    total_loaded = 0


    for warc_path, entries in list(warc_index.items()):
        try:
            # Pass full entry metadata to the extractor
            extracted = download_and_extract_company_data(
                entries=entries,  # ⬅ Updated: pass entries directly
                digest_set={e["digest"] for e in entries})
            if extracted:
                print(f"✅ Extracted {len(extracted)} records from {warc_path}")
                load_crawl_records(extracted)
                total_loaded += len(extracted)
        except Exception as e:
            print(f"[!] Failed to process {warc_path}: {e}")

    print(f"  ✔ Loaded {total_loaded} enriched company records to DB")


def run_all_parallel(run_abr=True, run_crawl=True, abr_limit=3, abr_records=None, crawl_pages=3):
    print("🧱 Creating database tables...")
    Base.metadata.create_all(engine)

    threads = []
    if run_abr:
        threads.append(threading.Thread(target=run_abr_pipeline, args=(abr_limit, abr_records)))
    if run_crawl:
        threads.append(threading.Thread(target=run_common_crawl_pipeline, args=(crawl_pages,)))

    print("🚀 Starting data pipelines...")
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print("\n✅ All data pipelines completed.")

def run_dbt_command(command: str, dbt_path: str, dbt_target: str = None):
    print(f"\n⚙️ Running: `dbt {command}` ...")
    cmd = ["dbt", command]
    if dbt_target:
        cmd += ["--target", dbt_target]

    env = os.environ.copy()
    env["DBT_PROJECT_DIR"] = dbt_path
    try:
        subprocess.run(cmd, cwd=dbt_path, check=True, env=env)
        print(f"✅ dbt {command} completed.")
    except subprocess.CalledProcessError as e:
        print(f"❌ dbt {command} failed:\n{e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data pipeline and dbt workflow")
    parser.add_argument("--abr", action="store_true", help="Run ABR pipeline")
    parser.add_argument("--crawl", action="store_true", help="Run Common Crawl pipeline")
    parser.add_argument("--abr-limit", type=int, default=3, help="Limit number of ABR XML files")
    parser.add_argument("--crawl-pages", type=int, default=3, help="Number of Common Crawl pages to fetch")
    parser.add_argument("--abr-records", type=int, help="Limit number of ABR records to load")
    parser.add_argument("--entity-matching", action="store_true", help="Perform entity matching after loading data")

    parser.add_argument("--run-dbt", action="store_true", help="Run dbt models")
    parser.add_argument("--test-dbt", action="store_true", help="Run dbt tests")
    parser.add_argument("--dbt-path", default="dbt", help="Path to dbt project directory")
    parser.add_argument("--dbt-target", default=None, help="dbt target profile (optional)")

    args = parser.parse_args()

    run_all_parallel(
        run_abr=args.abr,
        run_crawl=args.crawl,
        abr_limit=args.abr_limit,
        crawl_pages=args.crawl_pages,
        abr_records=args.abr_records
    )

    if args.entity_matching:
        print("\n🔍 Starting entity matching using vector similarity...")
        perform_string_matching()

    if args.run_dbt:
        run_dbt_command("run", args.dbt_path, args.dbt_target)

    if args.test_dbt:
        run_dbt_command("test", args.dbt_path, args.dbt_target)

