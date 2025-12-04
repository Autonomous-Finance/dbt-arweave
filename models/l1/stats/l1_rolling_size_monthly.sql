{{
    config(
        materialized='table',
        engine='MergeTree',
    )
}}

SELECT
    created_date,
    SUM(size) OVER (ORDER BY created_date) AS rolling_l1_size
FROM {{ref('l1_summary_stats_monthly')}}
{% if is_incremental() %}
WHERE created_date >= (select max(created_date) from {{ this }})
{% endif %}