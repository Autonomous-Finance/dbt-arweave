{{
    config(
        materialized='table',
        engine='MergeTree',
    )
}}


SELECT
    toStartOfMonth(first_seen_dt) AS first_seen_dt,
    MAX(rolling_count) AS total_rolling_monthly_count
FROM {{ref('l1_rolling_users')}}
{% if is_incremental() %}
WHERE first_seen_dt >= (select max(first_seen_dt) from {{ this }})
{% endif %}
GROUP BY 1