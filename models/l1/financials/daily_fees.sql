{{
    config(
        materialized='table',
        engine='ReplacingMergeTree',
        order_by='block_date'
    )
}}

select
    toStartOfDay(created_at) as block_date,
    sum(reward) as fees
from transactions_silver_tbl
group by block_date
order by block_date