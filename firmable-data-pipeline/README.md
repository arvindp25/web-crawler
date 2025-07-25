# Firmable Data Pipeline

This project is a technical assessment for the role of **Data Engineer** at Firmable. It implements a data pipeline that extracts, transforms, and loads Australian company data from **Common Crawl** and the **Australian Business Register (ABR)** into a PostgreSQL database, with entity matching and LLM integration to create a unified view of companies.

---

## 📌 Overview

### Data Sources:
- **Common Crawl (March 2025 Index)**  
  → Extracts Australian business website data (e.g., `URL`, `Company Name`, `Industry`).
- **Australian Business Register (ABR)**  
  → Parses bulk XML data including `ABN`, `Entity Name`, `Status`, `Address`, etc.

---

## 🏗️ Pipeline Architecture

> 📍 See [`architecture/pipeline_diagram.png`](architecture/pipeline_diagram.png) for visual overview.

1. **Extract**  
   - `extract/common_crawl_extractor.py`  
   - `extract/abr_extractor.py`

2. **Transform**  
   - `transform/entity_matching.py`  
   - `ai/llm_entity_matcher.py` (optional LLM-enhanced matching)

3. **Load**  
   - `load/postgres_loader.py`

4. **Postgres Storage**  
   - PostgreSQL schema: [`db/schema.sql`](db/schema.sql)  
   - Indexing, deduplication, and normalization.

5. **dbt Layer**  
   - Staging and final models (`stg_common_crawl`, `stg_abr`, `dim_companies`)  
   - Data quality tests

---

## 🧠 LLM Usage

- Used **OpenAI GPT-4** for enhanced fuzzy entity matching between ABR and Common Crawl records.
- Example prompt:


- File: [`ai/llm_entity_matcher.py`](ai/llm_entity_matcher.py)  
- Prompts are logged and include match confidence scores.

---

## 🧰 Technology Justification

| Component      | Tool         | Why |
|----------------|--------------|-----|
| Extract        | `requests`, `lxml`, `warcio` | Standard parsing and web archiving tools |
| Transform      | `pandas`, `fuzzywuzzy`, `openai` | Fast and flexible for matching logic |
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
