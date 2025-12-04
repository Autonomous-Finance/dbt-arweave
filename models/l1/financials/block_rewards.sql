{{
    config(
        materialized='table',
        engine='ReplacingMergeTree',
        order_by='block_date'
    )
}}

select
    block_date,
    reward
from default.viewblock_rewards vr
where block_date < '2024-02-01'
union all
select
    toStartOfDay(block_timestamp) as block_date,
    sum(toUInt64OrDefault(data_raw.reward, toUInt64(0)) / 1000000000000) as reward
from default.blocks
where height >= 1354815
group by block_date