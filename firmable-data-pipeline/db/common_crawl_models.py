from sqlalchemy import Column, String, Text, UniqueConstraint
from db.base import Base

class CrawlRecord(Base):
    __tablename__ = "crawl_records_extracted"

    url = Column(String, primary_key=True)
    title = Column(String)
    text = Column(Text)
    timestamp = Column(String)
    company_name = Column(String)
    digest = Column(String)
    __table_args__ = (UniqueConstraint("url", name="_url_uc"),)