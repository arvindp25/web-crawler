from db.abr_models import ABRRecord
from db.common_crawl_models import CrawlRecord
from db.conn import SessionLocal
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert
BATCH_SIZE = 500
def load_abr_records(records):
    session = SessionLocal()
    try:
        for r in records:
            abn = r.get("abn")
            if not abn:
                continue
            record = ABRRecord(**r)
            session.merge(record)  # upsert: insert if not exists, else update

        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        raise e
    finally:
        session.close()

def load_crawl_records(records):
    session = SessionLocal()
    try:
        valid_records = [r for r in records if r.get("digest") and r.get("url")]
        for i in range(0, len(valid_records), BATCH_SIZE):
            batch = valid_records[i:i + BATCH_SIZE]

            stmt = insert(CrawlRecord).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=["url"],
                set_={
                    "title": stmt.excluded.title,
                    "text": stmt.excluded.text,
                    "timestamp": stmt.excluded.timestamp
                },
                where=stmt.excluded.timestamp > CrawlRecord.timestamp  # only update if timestamp is newer
            )
            session.execute(stmt)

        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        raise e
    finally:
        session.close()