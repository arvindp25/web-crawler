-- 
-- Model: abr_preprocess
--
-- Description:
-- This model reads raw ABR (Australian Business Register) entity records from the source table 
-- `abr_records_extracted` and performs normalization of the `entity_name` field to support 
-- downstream fuzzy matching and entity resolution. 
--
-- Steps:
-- 1. Selects all relevant fields from the raw source.
-- 2. Adds intermediate cleaning columns:
--    - `lower_name`: Lowercased version of entity name.
--    - `no_punct_name`: Lowercased name with punctuation removed.
--    - `no_punct_whitespace_name`: Punctuation removed + multiple spaces reduced to single.
-- 3. Applies normalization to remove common business suffixes like "pty", "ltd", "co", etc.
--    to generate `normalized_name`, which will be used in embeddings and cosine similarity.
-- 4. Returns all original fields along with `normalized_name` as output.
--
-- Output:
-- One cleaned and normalized record per ABN, stored as a **physical table** for downstream use.
--

WITH raw AS (
    SELECT
        abn,
        entity_name,
        entity_type,
        entity_status,
        address,
        postcode,
        state,
        start_date,
        record_updated
    FROM {{ source('firmable_sources', 'abr_records_extracted') }}
),

cleaned AS (
    SELECT
        *,
        LOWER(entity_name) AS lower_name,
        REGEXP_REPLACE(LOWER(entity_name), '[^\w\s]', '', 'g') AS no_punct_name,
        REGEXP_REPLACE(
            REGEXP_REPLACE(LOWER(entity_name), '[^\w\s]', '', 'g'),
            '\s+', ' ', 'g'
        ) AS no_punct_whitespace_name
    FROM raw
)

SELECT
    abn,
    entity_name,
    entity_type,
    entity_status,
    address,
    postcode,
    state,
    start_date,
    record_updated,
    TRIM(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(no_punct_whitespace_name, '\bpty\b', '', 'gi'),
                                '\bltd\b', '', 'gi'
                            ),
                            '\blimited\b', '', 'gi'
                        ),
                        '\baustralia\b', '', 'gi'
                    ),
                    '\bthe\b', '', 'gi'
                ),
                '\bgroup\b', '', 'gi'
            ),
            '\bco\b', '', 'gi'
        )
    ) AS normalized_name
FROM cleaned