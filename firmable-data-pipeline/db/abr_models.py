from sqlalchemy import Column, String, Date, Integer
from db.base import Base

class ABRRecord(Base):
    __tablename__ = "abr_records_extracted"

    abn = Column(String, primary_key=True)
    entity_name = Column(String)
    entity_type = Column(String)
    entity_status = Column(String)
    address = Column(String)
    postcode = Column(String)
    state = Column(String)
    start_date = Column(Date)
    record_updated = Column(String)