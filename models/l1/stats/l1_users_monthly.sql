{{
    config(
        materialized='table',
        engine='AggregatingMergeTree',
    )
}}

SELECT
    toDate(toStartOfMonth(created_at)) AS created_month,
    uniqState(owner_address) AS monthly_users
FROM {{ ref('transactions_silver_tbl') }}
{% if is_incremental() %}
WHERE created_at >= (select max(created_month) from {{ this }})
{% endif %}
GROUP BY 1