{{
    config(
        materialized='table',
        engine='ReplacingMergeTree',
        order_by='block_date'
    )
}}
with q as (
    select
        br.block_date,
        endowment_size,
        reward
    from {{ ref('block_rewards') }} br
    left join {{ ref('endowment') }} e on e.block_date = br.block_date
)
select
    block_date,
   endowment_size,
   reward,
   endowment_size - lagInFrame(endowment_size) over (order by block_date) as endowment_delta,
   df.fees as fees
from q
left join {{ ref('daily_fees') }} df on df.block_date = q.block_date
