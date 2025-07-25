import threading
import argparse
import time
from db.conn import engine
from db.base import Base
from extract.abr_extractor import extract_abr_records, download_and_extract_abr_zip
from extract.common_crawl_extractor import search_common_crawl
from load.loader import load_abr_records, load_crawl_records

BATCH_SIZE = 500


def run_abr_pipeline(abr_limit=3):
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
    if batch:
        load_abr_records(batch)
        count += len(batch)
        print(f"  ✔ Loaded total {count} ABR records.")
    if count == 0:
        print("⚠️ No ABR records were loaded. Check XML contents or parsing logic.")


def run_common_crawl_pipeline(crawl_pages=3):
    print("\n🌍 Fetching Common Crawl data...")
    domain = "com.au"
    records = list(search_common_crawl(domain, pages=crawl_pages))
    print(f"  ✔ Fetched {len(records)} crawl records")
    load_crawl_records(records)
    print(f"  ✔ Loaded {len(records)} crawl records to DB")


def run_all_parallel(run_abr=True, run_crawl=True, abr_limit=3, crawl_pages=3):
    print("🧱 Creating database tables...")
    Base.metadata.create_all(engine)

    threads = []
    if run_abr:
        threads.append(threading.Thread(target=run_abr_pipeline, args=(abr_limit,)))
    if run_crawl:
        threads.append(threading.Thread(target=run_common_crawl_pipeline, args=(crawl_pages,)))

    print("🚀 Starting data pipelines...")
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print("\n✅ All data pipelines completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ABR and/or Common Crawl data pipelines")
    parser.add_argument("--abr", action="store_true", help="Run ABR pipeline")
    parser.add_argument("--crawl", action="store_true", help="Run Common Crawl pipeline")
    parser.add_argument("--abr-limit", type=int, default=3, help="Limit number of ABR XML files")
    parser.add_argument("--crawl-pages", type=int, default=3, help="Number of Common Crawl pages to fetch")

    args = parser.parse_args()

    if not args.abr and not args.crawl:
        print("ℹ️ No pipeline specified. Running both by default.")
        args.abr = args.crawl = True

    run_all_parallel(
        run_abr=args.abr,
        run_crawl=args.crawl,
        abr_limit=args.abr_limit,
        crawl_pages=args.crawl_pages
    )
