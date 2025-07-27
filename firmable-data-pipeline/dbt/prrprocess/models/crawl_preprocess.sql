{{ config(materialized='table') }}

-- This model cleans and normalizes company names from crawl_records_extracted
-- for downstream entity matching by:
-- - lowercasing
-- - removing punctuation
-- - normalizing whitespace
-- - stripping common legal suffixes (e.g., Pty, Ltd, etc.)
-- The final output includes all original columns plus a normalized company name.

with base as (
    select
        url,
        title,
        text,
        timestamp,
        company_name,
        digest
    from {{ source('firmable_sources', 'crawl_records_extracted') }}
),

cleaned as (
    select
        url,
        title,
        text,
        timestamp,
        digest,
        company_name,
        lower(company_name) as name_lower
    from base
),

punctuation_removed as (
    select
        url,
        title,
        text,
        timestamp,
        digest,
        company_name,
        regexp_replace(name_lower, '[^\w\s]', '', 'g') as name_no_punct
    from cleaned
),

whitespace_normalized as (
    select
        url,
        title,
        text,
        timestamp,
        digest,
        company_name,
        regexp_replace(name_no_punct, '\s+', ' ', 'g') as name_normalized
    from punctuation_removed
),

suffix_stripped as (
    select
        url,
        title,
        text,
        timestamp,
        digest,
        company_name,
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
    url,
    title,
    text,
    timestamp,
    digest,
    company_name,
    normalized_name
from suffix_stripped
where normalized_name is not null and length(normalized_name) > 1
