{{
    config(
        materialized='table',
        engine='ReplacingMergeTree',
        order_by='block_date'
    )
}}

select
    toStartOfDay(block_timestamp) as block_date,
    MAX(height) AS max_height,
    MAX(toUInt64OrDefault(data_raw.reward_pool, toUInt64(0)) / 1000000000000) AS endowment_size
from default.blocks
group by block_date
order by block_date desc