{{
    config(
        materialized='table',
        engine='MergeTree'
    )
}}

SELECT
    toStartOfQuarter(created_at) AS created_dt_quarter,
    tag_app_agg,
    COUNT(DISTINCT owner_address) AS quarterly_distinct_users
FROM {{ref('transactions_silver_tbl')}}
{% if is_incremental() %}
WHERE created_at >= (select max(created_dt_quarter) from {{ this }})
{% endif %}
GROUP BY 1, 2