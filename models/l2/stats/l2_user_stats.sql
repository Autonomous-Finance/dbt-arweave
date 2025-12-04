{{
    config(
        materialized='incremental',
        engine='MergeTree',
        order_by='first_seen_dt',
        unique_key='first_seen_dt'
    )
}}

SELECT
    owner_address,
    MIN(created_date) as first_seen_dt,
    MAX(created_date) as last_seen_dt,
    count(1) AS num_tx,
    SUM(is_smartweave_action) AS num_actions,
    SUM(is_smartweave_contract) AS num_contracts
FROM {{ref('l2_base')}}
{% if is_incremental() %}
WHERE created_date >= (select max(first_seen_dt) from {{ this }})
{% endif %}
GROUP BY 1