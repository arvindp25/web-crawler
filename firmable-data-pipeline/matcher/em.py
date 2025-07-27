from rapidfuzz import fuzz
from db.models import MatchedEntity
from db.conn import SessionLocal
from sqlalchemy import text
BATCH_SIZE = 512
MATCH_THRESHOLD = 85
ABR_TABLE = "abr_preprocess"
CRAWL_TABLE = "crawl_preprocess"

def perform_string_matching():
    session = SessionLocal()

    crawl_offset = 0
    total_matches = 0

    while True:
        # Batch crawl data
        crawl_batch = session.execute(
            text("""
                SELECT url, company_name, normalized_name
                        FROM crawl_preprocess
                        WHERE company_name IS NOT NULL
                        ORDER BY url
                        OFFSET :offset LIMIT :limit
                    """),
            {"offset": crawl_offset, "limit": BATCH_SIZE}
        ).fetchall()

        if not crawl_batch:
            break

        print(f"🔄 Processing crawl batch at offset {crawl_offset}...")

        for url, crawl_name, crawl_norm in crawl_batch:
            best_score = 0
            best_abn = None
            best_entity_name = None

            # Now scan ABR in batches (inner loop)
            abr_offset = 0
            while True:
                abr_batch = session.execute(
                            text("""
                                SELECT abn, entity_name, normalized_name
                                FROM abr_preprocess
                                WHERE entity_name IS NOT NULL
                                ORDER BY abn
                                OFFSET :abr_offset LIMIT :abr_limit
                            """),
                            {"abr_offset": abr_offset, "abr_limit": BATCH_SIZE}
                        ).fetchall()

                if not abr_batch:
                    break

                for abn, entity_name, abr_norm in abr_batch:
                    score = fuzz.token_set_ratio(crawl_norm, abr_norm)
                    if score > best_score:
                        best_score = score
                        best_abn = abn
                        best_entity_name = entity_name

                abr_offset += BATCH_SIZE

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
        crawl_offset += BATCH_SIZE

    session.close()
    print(f"🎉 Matching complete. Total matches stored: {total_matches}")
