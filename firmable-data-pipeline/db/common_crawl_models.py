from sqlalchemy import Column, String
from db.base import Base

class CrawlRecord(Base):
    __tablename__ = "crawl_records"

    digest = Column(String, primary_key=True)
    url = Column(String)
    timestamp = Column(String)
    mime = Column(String)
    status = Column(String)
    industry = Column(String, nullable=True)
