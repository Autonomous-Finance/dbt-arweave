{{
    config(
        materialized='table',
        engine='MergeTree',
        order_by='created_date',
    )
}}

SELECT
    created_date,
    sum(transactions) as transactions,
    sum(data_size) as data_size,
    max(max_date) as max_date_d,
    min(min_date) as min_date_d,
    toUnixTimestamp(max_date_d) - toUnixTimestamp(min_date_d) as seconds,
    -- if there is just one block of data assume 120 seconds
    transactions / if(seconds = 0, 120, seconds) as tps
FROM {{ ref('redstone_daily_detailed') }}
GROUP BY 1
ORDER BY 1