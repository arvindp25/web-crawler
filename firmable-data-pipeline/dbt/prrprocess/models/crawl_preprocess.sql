-- models/crawl_cleaned.sql

with base as (
    select
        id,
        company_name,
        url,
        industry,
        created_at
    from {{ ref('crawl_records') }}
),

cleaned as (
    select
        id,
        url,
        industry,
        created_at,
        lower(company_name) as name_lower
    from base
),

punctuation_removed as (
    select
        id,
        url,
        industry,
        created_at,
        regexp_replace(name_lower, '[^\w\s]', '', 'g') as name_no_punct
    from cleaned
),

whitespace_normalized as (
    select
        id,
        url,
        industry,
        created_at,
        regexp_replace(name_no_punct, '\s+', ' ', 'g') as name_normalized
    from punctuation_removed
),

suffix_stripped as (
    select
        id,
        url,
        industry,
        created_at,
        trim(
            regexp_replace(
                name_normalized,
                '\b(proprietary|pty|limited|ltd|inc|incorporated|australia|company|co|corp|corporation|plc)\b',
                '',
                'gi'
            )
        ) as normalized_name
    from whitespace_normalized
)

select
    id,
    url,
    industry,
    created_at,
    normalized_name
from suffix_stripped
where normalized_name is not null and length(normalized_name) > 1;
