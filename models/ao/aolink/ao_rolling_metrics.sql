{{
    config(
        materialized='table',
        engine='MergeTree',
        order_by='created_date',
        unique_key='created_date'
    )
}}
select
    created_date,
    sum(tx_count) over (order by created_date) as tx_count_rolling,
    sum(new_process_count) over (order by created_date) as processes_rolling,
    sum(new_module_count) over (order by created_date) as modules_rolling
from {{ ref('ao_metrics') }}
order by created_date