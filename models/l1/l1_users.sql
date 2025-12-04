{{
    config(
        materialized='materialized_view',
        incremental_strategy='append',
        engine='ReplacingMergeTree',
        order_by='created_at, tag_app, owner_address',
    )
}}
SELECT
    toStartOfHour(created_at) AS created_at,
    tag_app_agg AS tag_app,
    owner_address
FROM transactions_silver_tbl
WHERE NOT is_bundle
GROUP BY
    1,
    2,
    3