-- 1. Enable pg_trgm extension
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 2. GIN Indexes for fuzzy matching
CREATE INDEX IF NOT EXISTS idx_abr_entity_name_trgm
  ON abr_records_extracted USING gin (entity_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_crawl_company_name_trgm
  ON crawl_records_extracted USING gin (company_name gin_trgm_ops);

-- 3. Create read-only user
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT FROM pg_catalog.pg_roles WHERE rolname = 'readonly_user'
  ) THEN
    CREATE ROLE readonly_user LOGIN PASSWORD 'readonly123';
  END IF;
END
$$;

-- 4. Grant permissions to read-only user
GRANT CONNECT ON DATABASE firmable TO readonly_user;
GRANT USAGE ON SCHEMA public TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO readonly_user;


-- Run this once
CREATE EXTENSION IF NOT EXISTS vector;

-- Table for ABR embeddings
CREATE TABLE abr_embeddings (
    abn TEXT PRIMARY KEY REFERENCES abr_records_extracted(abn),
    entity_name TEXT,
    entity_embedding vector(384)
);

-- Table for Crawl embeddings
CREATE TABLE crawl_embeddings (
    url TEXT PRIMARY KEY REFERENCES crawl_records_extracted(url),
    company_name TEXT,
    company_embedding vector(384)
);

-- Optional indexes for ANN search
CREATE INDEX ON abr_embeddings USING ivfflat (entity_embedding vector_cosine_ops);
CREATE INDEX ON crawl_embeddings USING ivfflat (company_embedding vector_cosine_ops);