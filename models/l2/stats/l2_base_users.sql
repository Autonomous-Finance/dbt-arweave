{{
    config(
        materialized='materialized_view',
        incremental_strategy='append',
        engine='ReplacingMergeTree',
        order_by='created_at, owner_address',
    )
}}
SELECT
    toStartOfHour(created_at) AS created_at,
    owner_address
FROM transactions_silver_tbl
WHERE is_smartweave_action OR is_smartweave_contract
GROUP BY
    1,
    2