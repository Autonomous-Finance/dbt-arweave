{{
    config(
        materialized='table',
        engine='MergeTree',
    )
}}
WITH subset AS (
    SELECT
        tag_app_agg,
        toDate(created_at) AS created_date,
        SUM(1) AS transactions,
        COUNT(DISTINCT owner_address) AS users,
        SUM(data_size) AS data_size
    FROM {{ref('transactions_silver_tbl')}}
    WHERE created_at >= NOW() - INTERVAL 60 DAY
    GROUP BY tag_app_agg, toDate(created_at)
), zero_dates AS (
    SELECT
    arrayJoin(arrayMap(x -> toDate(x), range(toUInt32(toStartOfDay(NOW() - INTERVAL 60 DAY)), toUInt32(toStartOfDay(NOW())), 24 * 3600))) as created_date,
    0 AS transactions,
    0 AS users,
    0 AS data_size
), with_apps AS (
   SELECT
    *
   FROM zero_dates
   CROSS JOIN (SELECT DISTINCT tag_app_agg FROM subset) _
), continous AS (
    SELECT
        tag_app_agg,
        created_date,
        coalesce(actual.transactions, zro.transactions) AS transactions,
        coalesce(actual.users, zro.users) AS users,
        coalesce(actual.data_size, zro.data_size) AS data_size
    FROM with_apps AS zro
    LEFT JOIN subset AS actual USING (tag_app_agg, created_date)
), delta AS (
    SELECT
        tag_app_agg,
        created_date,
        transactions,
        users,
        data_size,
        (transactions - any(transactions) OVER (PARTITION BY tag_app_agg ORDER BY created_date ROWS BETWEEN 30 PRECEDING AND 30 PRECEDING)) AS delta_transactions,
        (users - any(users) OVER (PARTITION BY tag_app_agg ORDER BY created_date ROWS BETWEEN 30 PRECEDING AND 30 PRECEDING)) AS delta_users,
        (data_size - any(data_size) OVER (PARTITION BY tag_app_agg ORDER BY created_date ROWS BETWEEN 30 PRECEDING AND 30 PRECEDING)) delta_data_size
    FROM continous
)
SELECT * FROM delta
WHERE created_date >= NOW() - INTERVAL 30 DAY