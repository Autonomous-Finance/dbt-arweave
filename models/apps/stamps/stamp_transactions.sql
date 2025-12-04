{{
    config(
        materialized='materialized_view',
        engine='ReplacingMergeTree',
        order_by='created_at, id'
    )
}}

select * from prod.transactions_silver_tbl where tag_app_agg = 'stamp'
