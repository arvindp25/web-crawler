from db.abr_models import ABRRecord
from db.common_crawl_models import CrawlRecord
from db.conn import SessionLocal
from sqlalchemy.exc import SQLAlchemyError
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
        valid_records = [CrawlRecord(**r) for r in records if r.get("digest")]
        for i in range(0, len(valid_records), BATCH_SIZE):
            batch = valid_records[i:i + BATCH_SIZE]
            session.add_all(batch)
            session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        raise e
    finally:
        session.close()
