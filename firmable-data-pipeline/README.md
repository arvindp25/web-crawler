# Firmable Data Pipeline

This project is a technical assessment for the role of **Data Engineer** at Firmable. It implements a data pipeline that extracts, transforms, and loads Australian company data from **Common Crawl** and the **Australian Business Register (ABR)** into a PostgreSQL database, with entity matching and LLM integration to create a unified view of companies.


## 🧰 Technology Justification

| Component      | Tool         | Why |
|----------------|--------------|-----|
| Extract        | `requests`, `lxml`, `warcio` | Standard parsing and web archiving tools |
| Transform      | `sql`, `fuzzywuzzy`, | Fast and flexible for matching logic |
| Load           | `SQLAlchemy`, `psycopg2` | Safe PostgreSQL interaction |
| Database       | PostgreSQL   | Reliable RDBMS for structured company data |
| Quality Layer  | `dbt-core`   | Declarative modeling + testing |
| Deployment     | CLI scripts  | Lightweight for this assessment |

---

## 🧪 Setup Instructions

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/firmable-data-pipeline.git
cd firmable-data-pipeline


# 📊 Firmable Data Pipeline

## 🔍 Overview

Design and implement a data pipeline to extract, transform, and load Australian company data from two sources—**Common Crawl** and the **Australian Business Register (ABR)**—into a **PostgreSQL** database. Perform **entity matching** to merge datasets and create a unified view of each company.

---

## 📚 Data Sources

### 🕸️ Common Crawl (March 2025 Index)

- Extracts data from Australian company websites (\~200,000 domains).
- **Fields Extracted**:
  - `URL`
  - `Company Name`
  - `title`
  - `text`
  - `Industry` -not available
- **Reference**: [https://commoncrawl.org/](https://commoncrawl.org/)

### 🏢 Australian Business Register (ABR)

- Processes bulk XML files.
- **Fields Extracted**:
  - `ABN`, `Entity Name`, `Entity Type`, `Entity Status`
  - `Address`, `Postcode`, `State`, `Start Date`, `record updated`
- **Reference**: [https://data.gov.au/](https://data.gov.au/)

---

## 🧱 Database Schema (PostgreSQL DDL)

```sql
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

-- 4. Optional: IVFFLAT indexes for ANN search (assumes table exists)
CREATE INDEX IF NOT EXISTS idx_abr_embedding_ann
  ON abr_embeddings USING ivfflat (entity_embedding vector_cosine_ops);

CREATE INDEX IF NOT EXISTS idx_crawl_embedding_ann
  ON crawl_embeddings USING ivfflat (company_embedding vector_cosine_ops);

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

---
```
## ⚙️ Pipeline Architecture
## 📊 Data Pipeline Architecture

```text
## 📊 Data Pipeline Architecture

```text
                           ┌──────────────────────────────┐
                           │        run.py CLI Tool       │
                           │  (Orchestrates the Pipeline) │
                           └────────────┬─────────────────┘
                                        │
         ┌────────────────────────────────────────────────────────────┐
         │                                                             │
         ▼                                                             ▼
┌────────────────────────────┐                     ┌────────────────────────────────┐
│    ABR XML Extractor       │                     │   Common Crawl Fetcher         │
│ (extract/abr_extractor.py) │                     │ (extract/common_crawl_extractor.py) │
└────────────┬───────────────┘                     └────────────┬────────────────────┘
             │                                                  │
             ▼                                                  ▼
   ┌────────────────────────────┐   ┌──────────────────────────────┐
   │    Parsed ABR Records      │   │   Enriched Crawl Records     │
   │ (XML → dict → ABRRecord)   │   │ (digest-based extraction)    │
   └────────────┬───────────────┘   └────────────┬────────────────┘
                │                               │
                ▼                               ▼
       ┌────────────────────────────┐   ┌────────────────────────────┐
       │   Load to PostgreSQL       │   │   Load to PostgreSQL       │
       │ (load/loader.py: ABR)      │   │ (load/loader.py: Crawl)    │
       └────────────┬───────────────┘   └────────────┬───────────────┘
                    │                               │
                    ▼                               ▼
         ┌────────────────────┐           ┌──────────────────────┐
         │  PostgreSQL (firmable)│         │ Tables: abr_record,   │
         │   via SQLAlchemy ORM  │         │        crawl_record   │
         └────────────────────┘           └──────────────────────┘

                                ▼
                   ┌───────────────────────────┐
                   │ DBT Transformations & Tests│
                   │     (in /dbt/prrprocess)   │
                   └────────────┬──────────────┘
                                │
                                ▼
                  ┌────────────────────────────────┐
                  │  Unified & Cleaned Models       │
                  │  (dbt run + dbt test results)   │
                  └────────────┬───────────────────┘
                               │
                               ▼
                  ┌──────────────────────────────┐
                  │ Matcher (EM) - Final Match     │
                  │ (matcher/em.py on dbt models) │
                  └────────────┬──────────────────┘
                               ▼
                  ┌──────────────────────────────┐
                  │ Final Matched Entity Results  │
                  │ (cosine sim over cleaned data)│
                  └──────────────────────────────┘
```

---

## 🏗️ Technology Justification

| Component       | Tech Used                           | Reason                                 |
| --------------- | ----------------------------------- | -------------------------------------- |
| Orchestration   | Python + threading                  | Lightweight, simple for parallel ETL   |
| Data Extraction | `warcio`, `bs4`, `requests`         | Efficient web archive and HTML parsing |
| Data Loading    | SQLAlchemy ORM                      | Flexibility with PostgreSQL            |
| Matching        | `fuzzy-wuzzy`                       | Light weight and less computational.   |
| Transformation  | dbt                                 | Modular, tested SQL pipelines          |
| Deployment      | Docker                              | Reproducible, portable environments    |

---

## 🧠 AI Model Used & Rationale

No, an AI model was not used directly in this project due to the lack of computational resources and financial constraints. While advanced AI models could enhance entity matching through semantic understanding, their use often requires high-performance hardware or paid subscriptions, which were not feasible in this context.
---

## 🚀 Setup & Running Instructions

### 1. Clone the Repo

```bash
clone the repo
cd firmable-data-pipeline
```

### 2. Run with Docker

```bash
docker-compose up --build
```

## 🧪 Data Quality & Matching Strategy

- **Cleaning**:
  - Lowercasing, whitespace trimming, stopword removal
  - Unicode normalization
- **Entity Matching**:
  - Use SentenceTransformer embeddings for semantic comparison
  - Compute cosine similarity between ABR & Crawl names
  - Store matches with high confidence in `matched_entities`
- **Quality Tests**:
  - dbt tests on nulls, duplicates
  - Integrity checks on foreign keys and formats

---

## 🔐 DB Security

- Separate roles for read/write
- Secure credentials in `.env` or Docker secrets
- pgAdmin exposed only locally for inspection

---

## 🛠️ IDE Used

- **Visual Studio Code (VSCode)**
  - Python extension
  - Docker + dbt integrations
  - pg admin is exposed at 5050 use localhost:5050 
  login: admin@firmable.com and pass:admin 
      a. create server name 'db'
      d. add connection 
         host-postgres
         user-admin
         password-admin


---

## 📬 Submission

- Public GitHub Repo: [https://github.com/](https://github.com/<your-org>/firmable-data-pipeline)[/firmable-data-pipeline](https://github.com/<your-org>/firmable-data-pipeline)

---

## 📃 License

MIT License

