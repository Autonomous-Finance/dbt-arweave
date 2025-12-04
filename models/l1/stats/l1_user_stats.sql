{{
    config(
        materialized='table',
        engine='MergeTree',
    )
}}

SELECT
    owner_address,
    MIN(toDate(created_at)) as first_seen_dt,
    MAX(toDate(created_at)) as last_seen_dt,
    count(1) AS num_tx,
    SUM(data_size) AS sum_size_upload,
    SUM(reward) AS sum_reward_paid,
    SUM(quantity) AS sum_ar_transacted
FROM {{ ref('transactions_silver_tbl') }}
{% if is_incremental() %}
WHERE created_at >= (select max(first_seen_dt) from {{ this }})
{% endif %}
GROUP BY 1