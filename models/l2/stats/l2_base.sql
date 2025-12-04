{{
    config(
        materialized='incremental',
        engine='ReplacingMergeTree',
        order_by='created_date,id',
    )
}}

SELECT id, toDate(created_at) AS created_date, owner_address, is_smartweave_action, is_smartweave_contract
FROM {{ref('transactions_silver_tbl')}}
WHERE is_smartweave_action OR is_smartweave_contract
{% if is_incremental() %}
AND created_at >= (select max(created_date) from {{ this }})
{% endif %}