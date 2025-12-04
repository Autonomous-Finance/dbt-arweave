{{
    config(
        materialized='table',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree',
        order_by='created_date',
        unique_key='created_date'
    )
}}
select
    toStartOfDay(created_at) AS created_date,
    count(1) as tx_count,
    countIf(get_tag('Action', tags) = 'Eval') as eval_count,
    countIf(get_tag('Action', tags) = 'Transfer') as transfer_count,
    countIf(get_tag('Type', tags) = 'Process') as new_process_count,
    countIf(get_tag('Type', tags) = 'Module') as new_module_count,
    count(distinct owner_address) as active_users,
    count(distinct get_tag('From-Process', tags)) as active_processes
from prod.transactions_silver_tbl final
where get_tag('Data-Protocol', tags) = 'ao' AND created_at > '2023-10-31'
group by created_date
order by created_date

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  AND created_date >= (select max(created_date) from {{ this }})

{% endif %}