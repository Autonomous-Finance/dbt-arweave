{{
    config(
        materialized='table',
        engine='SummingMergeTree',
        order_by='created_date'
    )
}}

SELECT
    toStartOfDay(created_at) AS created_date,
    SUM(cnt) AS total_count,
    SUM(has_any_ans110) AS has_any_ans110,
    SUM(has_ans110_title) AS has_ans110_title,
    SUM(has_ans110_type) AS has_ans110_type,
    SUM(has_ans110_topic) AS has_ans110_topic,
    SUM(has_ans110_description) AS has_ans110_description,
    SUM(has_render_with) AS has_render_with,
    SUM(has_udl) AS has_udl
FROM {{ ref('ans_counts') }}
GROUP BY created_date