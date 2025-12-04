{{
    config(
        materialized='incremental',
        order_by='created_at',
        incremental_strategy='append'
    )
}}

SELECT
    created_at,
    1 AS cnt,
    least(length(arrayFilter(t -> lower(t['name']) in ('title', 'type', 'topic', 'description'),
                                tags)), 1) AS has_any_ans110,
    least(length(arrayFilter(t -> lower(t['name']) in ('render-with'),
                                tags)), 1) AS has_render_with,
    least(length(arrayFilter(t -> lower(t['name']) in ('title'),
                                tags)), 1) AS has_ans110_title,
    least(length(arrayFilter(t -> lower(t['name']) in ('type'),
                                tags)), 1) AS has_ans110_type,
    least(length(arrayFilter(t -> lower(t['name']) like 'topic%',
                                tags)), 1) AS has_ans110_topic,
    least(length(arrayFilter(t -> lower(t['name']) in ('description'),
                                tags)), 1) AS has_ans110_description,
    toInt8(arrayFilter(t -> lower(t['name']) = 'license', tags)[1]['value'] = 'yRj4a5KMctX_uOmKWCFJIjmY8DeJcusVk6-HzLiM_t8') AS has_udl
FROM {{ ref('transactions_silver_tbl') }}
WHERE created_at >= '2022-06-01'
    {% if is_incremental() %}
    AND created_at > (SELECT MAX(created_at) from {{ this }})
    {% endif %}