{{
    config(
        materialized='table',
        engine='MergeTree',
    )
}}

SELECT
    first_seen_dt,
    COUNT(DISTINCT tag_app_agg) OVER (ORDER BY first_seen_dt) as rolling_count
FROM {{ ref('l1_protocol_stats') }}
{% if is_incremental() %}
WHERE first_seen_dt >= (select max(first_seen_dt) from {{ this }})
{% endif %}