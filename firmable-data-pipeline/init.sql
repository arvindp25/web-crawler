-- 1. Enable pg_trgm extension
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 2. Create tables first

CREATE TABLE abr_records_extracted (
    abn TEXT PRIMARY KEY,
    entity_name TEXT,
    entity_type TEXT,
    entity_status TEXT,
    address TEXT,
    postcode TEXT,
    state TEXT,
    start_date DATE,
    record_updated TEXT
);

CREATE TABLE crawl_records_extracted (
    url TEXT PRIMARY KEY,
    title TEXT,
    text TEXT,
    timestamp TEXT,
    company_name TEXT,
    digest TEXT UNIQUE,
    CONSTRAINT _url_uc UNIQUE (url)
);

CREATE TABLE matched_entities (
    abn TEXT REFERENCES abr_records_extracted(abn) ON DELETE CASCADE,
    url TEXT REFERENCES crawl_records_extracted(url) ON DELETE CASCADE,
    entity_name TEXT,
    company_name TEXT,
    similarity_score FLOAT,
    PRIMARY KEY (abn, url)
);

-- 3. Create B-tree and GIN indexes

-- B-tree
CREATE INDEX ix_abr_records_extracted_entity_name ON abr_records_extracted (entity_name);
CREATE INDEX ix_abr_records_extracted_postcode ON abr_records_extracted (postcode);
CREATE INDEX ix_abr_records_extracted_state ON abr_records_extracted (state);
CREATE INDEX ix_crawl_records_extracted_timestamp ON crawl_records_extracted (timestamp);
CREATE INDEX ix_crawl_records_extracted_company_name ON crawl_records_extracted (company_name);
CREATE INDEX ix_company_name_digest ON crawl_records_extracted (company_name, digest);
CREATE INDEX ix_similarity_entity_company ON matched_entities (similarity_score, entity_name, company_name);

-- GIN (Trigram for fuzzy matching)
CREATE INDEX IF NOT EXISTS idx_abr_entity_name_trgm
  ON abr_records_extracted USING gin (entity_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_crawl_company_name_trgm
  ON crawl_records_extracted USING gin (company_name gin_trgm_ops);


-- 5. Create read-only user and grant access

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT FROM pg_catalog.pg_roles WHERE rolname = 'readonly_user'
  ) THEN
    CREATE ROLE readonly_user LOGIN PASSWORD 'readonly123';
  END IF;
END
$$;

GRANT CONNECT ON DATABASE firmable TO readonly_user;
GRANT USAGE ON SCHEMA public TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO readonly_user;
