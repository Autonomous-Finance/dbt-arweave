{{
    config(
        materialized='materialized_view',
        incremental_strategy='append',
        engine='SummingMergeTree',
        order_by='created_date, agg_content_type, tag_main_content_type, tag_sub_content_type, tag_type,
                          tag_app',
    )
}}
SELECT
    toStartOfHour(created_at) AS created_date,
    agg_content_type,
    tag_main_content_type,
    tag_sub_content_type,
    tag_type,
    tag_app_agg AS tag_app,
    count(1) AS file_count,
    sum(data_size) AS file_size
FROM transactions_silver_tbl
WHERE NOT is_bundle
GROUP BY
    created_date,
    agg_content_type,
    tag_main_content_type,
    tag_sub_content_type,
    tag_type,
    tag_app
