{{
    config(
        materialized='materialized_view',
        incremental_strategy='append',
        engine='SummingMergeTree',
        order_by='content_type, app',
    )
}}
SELECT INITCAP(agg_content_type) AS content_type,
       CASE
           WHEN agg_content_type = 'Redstone Oracles' THEN 'Redstone'
           WHEN agg_content_type = 'JSON' THEN coalesce(NULLIF(INITCAP(tag_app), ''), 'Unknown App')
           ELSE coalesce(NULLIF(INITCAP(tag_app), ''), 'Unknown App')
       END AS app,
       sum(file_count) AS file_count,
       sum(file_size) AS file_size
FROM {{ ref('ar_tx_agg') }}
GROUP BY 1, 2