{{
    config(
        materialized='incremental',
        engine='MergeTree',
        order_by='first_seen_dt',
        unique_key='first_seen_dt'
    )
}}

SELECT
    first_seen_dt,
    COUNT(DISTINCT owner_address) OVER (ORDER BY first_seen_dt) as rolling_count
FROM {{ref('l2_user_stats')}}
{% if is_incremental() %}
WHERE first_seen_dt >= (select max(first_seen_dt) from {{ this }})
{% endif %}