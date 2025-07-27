-- models/abr_cleaned.sql

WITH raw AS (
    SELECT
        abn,
        entity_name
    FROM {{ ref('abr_records') }}
),

cleaned AS (
    SELECT
        abn,
        entity_name,
        LOWER(entity_name) AS lower_name,
        REGEXP_REPLACE(LOWER(entity_name), '[^\w\s]', '', 'g') AS no_punct_name,
        REGEXP_REPLACE(
            REGEXP_REPLACE(LOWER(entity_name), '[^\w\s]', '', 'g'),
            '\s+', ' ', 'g'
        ) AS no_punct_whitespace_name
)

SELECT
    abn,
    entity_name,
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
FROM cleaned;


-- models/crawl_cleaned.sql

WITH raw AS (
    SELECT
        url,
        company_name
    FROM {{ ref('crawl_records') }}
),

cleaned AS (
    SELECT
        url,
        company_name,
        LOWER(company_name) AS lower_name,
        REGEXP_REPLACE(LOWER(company_name), '[^\w\s]', '', 'g') AS no_punct_name,
        REGEXP_REPLACE(
            REGEXP_REPLACE(LOWER(company_name), '[^\w\s]', '', 'g'),
            '\s+', ' ', 'g'
        ) AS no_punct_whitespace_name
)

SELECT
    url,
    company_name,
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
FROM cleaned;