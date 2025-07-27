from rapidfuzz import fuzz
from db.models import ABRRecord
from db.models import CrawlRecord
from db.models import MatchedEntity
from db.conn import SessionLocal

BATCH_SIZE = 512
MATCH_THRESHOLD = 80  # Can be lowered to 85 if needed

def perform_string_matching():
    session = SessionLocal()

    # Fetch all ABR records once into memory (can optimize later if needed)
    abr_records = session.query(ABRRecord.abn, ABRRecord.entity_name).filter(ABRRecord.entity_name != None).all()
    print(f"✅ Loaded {len(abr_records)} ABR records into memory.")

    offset = 0
    total_matches = 0

    while True:
        crawl_batch = (
            session.query(CrawlRecord.url, CrawlRecord.company_name)
            .filter(CrawlRecord.company_name != None)
            .offset(offset)
            .limit(BATCH_SIZE)
            .all()
        )

        if not crawl_batch:
            break

        print(f"🔄 Processing crawl batch at offset {offset}...")

        for url, crawl_name in crawl_batch:
            best_score = 0
            best_abn = None
            best_entity_name = None

            for abn, entity_name in abr_records:
                score = fuzz.token_set_ratio(crawl_name, entity_name)
                if score > best_score:
                    best_score = score
                    best_abn = abn
                    best_entity_name = entity_name

            if best_score >= MATCH_THRESHOLD:
                match = MatchedEntity(
                    abn=best_abn,
                    url=url,
                    entity_name=best_entity_name,
                    company_name=crawl_name,
                    similarity_score=best_score
                )
                session.merge(match)
                total_matches += 1

        session.commit()
        offset += BATCH_SIZE

    session.close()
    print(f"🎉 Matching complete. Total matches stored: {total_matches}")
