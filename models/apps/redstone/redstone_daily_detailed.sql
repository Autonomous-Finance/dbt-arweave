{{
    config(
        materialized='materialized_view',
        incremental_strategy='append',
        engine='SummingMergeTree',
        order_by='created_date, data_service_id, data_feed_id',
    )
}}
SELECT
    toStartOfDay(created_at) AS created_date,
    data_service_id,
    data_feed_id,
    count(1) AS transactions,
    min(created_at) as min_date,
    max(created_at) as max_date,
    sum(data_size) as data_size
FROM {{ ref('redstone_transactions') }}
WHERE data_service_id != 'mock-data-service-id'
GROUP BY created_date, data_service_id, data_feed_id