{{
    config(
        materialized='table',
        order_by='created_date'
    )
}}

SELECT
    toStartOfDay(created_at) AS created_date,
    owner_address,
    arrayFilter(t -> lower(t['name']) = 'vouch-for', tags)[1]['value'] AS vouched_user
FROM {{ ref('transactions_silver_tbl') }}
WHERE vouched_user != ''