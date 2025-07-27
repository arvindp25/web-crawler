from sqlalchemy import (
    Column, String, Float, Date, Text, ForeignKey,
    UniqueConstraint, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from db.base import Base

class ABRRecord(Base):
    __tablename__ = "abr_records_extracted"

    abn = Column(String, primary_key=True)
    entity_name = Column(String, index=True)
    entity_type = Column(String)
    entity_status = Column(String)
    address = Column(String)
    postcode = Column(String, index=True)
    state = Column(String, index=True)
    start_date = Column(Date)
    record_updated = Column(String)

    matches = relationship("MatchedEntity", back_populates="abr_record", cascade="all, delete")


class CrawlRecord(Base):
    __tablename__ = "crawl_records_extracted"

    url = Column(String, primary_key=True)
    title = Column(String)
    text = Column(Text)
    timestamp = Column(String, index=True)
    company_name = Column(String, index=True)
    digest = Column(String, unique=True)

    __table_args__ = (
        UniqueConstraint("url", name="_url_uc"),
        Index("ix_company_name_digest", "company_name", "digest"),
    )

    matches = relationship("MatchedEntity", back_populates="crawl_record", cascade="all, delete")


class MatchedEntity(Base):
    __tablename__ = "matched_entities"

    abn = Column(String, ForeignKey("abr_records_extracted.abn"), primary_key=True)
    url = Column(String, ForeignKey("crawl_records_extracted.url"), primary_key=True)
    entity_name = Column(String)
    company_name = Column(String)
    similarity_score = Column(Float, index=True)

    abr_record = relationship("ABRRecord", back_populates="matches")
    crawl_record = relationship("CrawlRecord", back_populates="matches")

    __table_args__ = (
        Index("ix_similarity_entity_company", "similarity_score", "entity_name", "company_name"),
    )
