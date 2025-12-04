{{
    config(
        materialized='table',
        engine='SummingMergeTree',
    )
}}

SELECT
    toStartOfMonth(created_at) AS created_date,
    count(1) AS l1_transactions,
    SUM(CASE WHEN agg_content_type = 'Transaction' THEN 1 ELSE 0 END) AS ar_transactions,
    SUM(CASE WHEN data_size > 5000 THEN 1 ELSE 0 END) AS files,
    SUM(data_size) AS size
FROM {{ ref('transactions_silver_tbl') }}
WHERE NOT is_bundle
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  AND created_at >= (select max(created_date) from {{ this }})

{% endif %}
GROUP BY 1