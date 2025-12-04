{{
    config(
        materialized='materialized_view',
        engine='SummingMergeTree',
        order_by='created_at'
    )
}}
SELECT
    toStartOfHour(created_at) AS created_at,
    countIf(is_smartweave_action) AS smartweave_actions,
    countIf(is_smartweave_contract) AS smartweave_contracts
FROM {{ ref('transactions_silver_tbl') }}
WHERE is_smartweave_action OR is_smartweave_contract
GROUP BY 1
