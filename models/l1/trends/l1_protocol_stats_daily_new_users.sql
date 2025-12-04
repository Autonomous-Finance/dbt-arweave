{{
    config(
        materialized='incremental',
        engine='MergeTree',
        order_by='tag_app_agg, created_date',
        unique_key='tag_app_agg, created_date',
    )
}}
SELECT
    tag_app_agg,
    toDate(created_at) as created_date,
    COUNT(DISTINCT CASE WHEN first_seen_dt = toDate(created_at) THEN owner_address END) as new_users
FROM {{ref('transactions_silver_tbl')}}
LEFT JOIN {{ref('l1_protocol_user_stats')}} USING (owner_address, tag_app_agg)
{% if is_incremental() %}
WHERE created_at >= (select max(created_date) from {{ this }})
{% endif %}
GROUP BY 1, 2