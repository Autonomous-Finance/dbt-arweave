{{
    config(
        materialized='table',
        engine='MergeTree',
        order_by='owner_address, first_seen_dt'
    )
}}

SELECT
    owner_address,
    tag_app_agg,
    MIN(toDate(created_at)) as first_seen_dt,
    MAX(toDate(created_at)) as last_seen_dt,
    count(1) AS num_tx
FROM {{ref('transactions_silver_tbl')}}
GROUP BY 1, 2