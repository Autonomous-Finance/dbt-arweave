{{
    config(
        materialized='table',
        engine='MergeTree',
        order_by='tx_count',
    )
}}
SELECT INITCAP(tag_app) AS app, count(1) AS tx_count
FROM {{ ref('ar_tx_agg') }}
WHERE app != ''
GROUP BY 1
ORDER BY 2 DESC