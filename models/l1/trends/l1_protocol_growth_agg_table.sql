{{
    config(
        materialized='table',
        engine='MergeTree',
        order_by='tag_app_agg'
    )
}}
WITH base_agg AS (
    SELECT
        CASE
            WHEN tag_app_agg LIKE 'signal_v2%' THEN 'signal_v2'
            WHEN tag_app_agg LIKE 'bandplay%' THEN 'bandplay'
            WHEN tag_app_agg LIKE 'spotlight_comment%' THEN 'spotlight_comment'
            ELSE tag_app_agg
        END AS tag_app_agg,
        MIN(toDate(created_at)) AS first_seen_date,
        MAX(toDate(created_at)) AS last_seen_date,
        toDate(NOW()) - MIN(toDate(created_at)) AS protocol_age,
        COUNT(1) AS total_tx,
        COUNT(DISTINCT owner_address) AS total_users,
        SUM(data_size) AS total_size,
        countIf(1, created_at >= now() - INTERVAL 7 DAY) AS num_tx_7d,
        countIf(1, created_at >= now() - INTERVAL 14 DAY) AS num_tx_14d,
        countIf(1, created_at >= now() - INTERVAL 30 DAY) AS num_tx_30d,
        countIf(DISTINCT owner_address, created_at >= now() - INTERVAL 7 DAY) AS num_users_7d,
        countIf(DISTINCT owner_address, created_at >= now() - INTERVAL 14 DAY) AS num_users_14d,
        countIf(DISTINCT owner_address, created_at >= now() - INTERVAL 30 DAY) AS num_users_30d,
        sumIf(data_size, created_at >= now() - INTERVAL 7 DAY) AS sum_size_7d,
        sumIf(data_size, created_at >= now() - INTERVAL 14 DAY) AS sum_size_14d,
        sumIf(data_size, created_at >= now() - INTERVAL 30 DAY) AS sum_size_30d
    FROM {{ref('transactions_silver_tbl')}}
    WHERE tag_app_agg != ''
    GROUP BY tag_app_agg
), new_users_agg AS (
    SELECT
        tag_app_agg,
        sumIf(new_users, l1nu.created_date >= now() - INTERVAL 7 DAY) AS new_users_7d,
        sumIf(new_users, l1nu.created_date >= now() - INTERVAL 14 DAY) AS new_users_14d,
        sumIf(new_users, l1nu.created_date >= now() - INTERVAL 30 DAY) AS new_users_30d
    FROM {{ref('l1_protocol_stats_daily_new_users')}} l1nu
    GROUP BY 1
), prev_base_agg AS (
    SELECT
        CASE
            WHEN tag_app_agg LIKE 'signal_v2%' THEN 'signal_v2'
            WHEN tag_app_agg LIKE 'bandplay%' THEN 'bandplay'
            WHEN tag_app_agg LIKE 'spotlight_comment%' THEN 'spotlight_comment'
            ELSE tag_app_agg
        END AS tag_app_agg,
        countIf(1, created_at BETWEEN now() - INTERVAL 7+7 DAY AND now() - INTERVAL 7 DAY) AS prev_num_tx_7d,
        countIf(1, created_at BETWEEN now() - INTERVAL 14+14 DAY AND now() - INTERVAL 14 DAY) AS prev_num_tx_14d,
        countIf(1, created_at BETWEEN now() - INTERVAL 30+30 DAY AND now() - INTERVAL 30 DAY) AS prev_num_tx_30d,
        countIf(DISTINCT owner_address, created_at BETWEEN now() - INTERVAL 7+7 DAY AND now() - INTERVAL 7 DAY) AS prev_num_users_7d,
        countIf(DISTINCT owner_address, created_at BETWEEN now() - INTERVAL 14+14 DAY AND now() - INTERVAL 14 DAY) AS prev_num_users_14d,
        countIf(DISTINCT owner_address, created_at BETWEEN now() - INTERVAL 30+30 DAY AND now() - INTERVAL 30 DAY) AS prev_num_users_30d,
        sumIf(data_size, created_at BETWEEN now() - INTERVAL 7+7 DAY AND now() - INTERVAL 7 DAY) AS prev_sum_size_7d,
        sumIf(data_size, created_at BETWEEN now() - INTERVAL 14+14 DAY AND now() - INTERVAL 14 DAY) AS prev_sum_size_14d,
        sumIf(data_size, created_at BETWEEN now() - INTERVAL 30+30 DAY AND now() - INTERVAL 30 DAY) AS prev_sum_size_30d
    FROM {{ref('transactions_silver_tbl')}}
    WHERE tag_app_agg != ''
    GROUP BY tag_app_agg
), prev_new_users_agg AS (
    SELECT
        tag_app_agg,
        sumIf(new_users, l1nu.created_date BETWEEN now() - INTERVAL 7+7 DAY AND now() - INTERVAL 7 DAY) AS prev_new_users_7d,
        sumIf(new_users, l1nu.created_date BETWEEN now() - INTERVAL 14+14 DAY AND now() - INTERVAL 14 DAY) AS prev_new_users_14d,
        sumIf(new_users, l1nu.created_date BETWEEN now() - INTERVAL 30+30 DAY AND now() - INTERVAL 30 DAY) AS prev_new_users_30d
    FROM {{ref('l1_protocol_stats_daily_new_users')}} l1nu
    GROUP BY 1
)
SELECT
    ba.tag_app_agg AS tag_app_agg,
    first_seen_date,
    last_seen_date,
    protocol_age,
    total_tx,
    total_users,
    total_size,
    num_tx_7d,
    num_tx_14d,
    num_tx_30d,
    num_users_7d,
    num_users_14d,
    num_users_30d,
    sum_size_7d,
    sum_size_14d,
    sum_size_30d,
    new_users_7d,
    new_users_14d,
    new_users_30d,
    prev_num_tx_7d,
    prev_num_tx_14d,
    prev_num_tx_30d,
    prev_num_users_7d,
    prev_num_users_14d,
    prev_num_users_30d,
    prev_sum_size_7d,
    prev_sum_size_14d,
    prev_sum_size_30d,
    prev_new_users_7d,
    prev_new_users_14d,
    prev_new_users_30d
FROM base_agg ba
JOIN new_users_agg nua ON ba.tag_app_agg = nua.tag_app_agg
JOIN prev_base_agg pa ON ba.tag_app_agg = pa.tag_app_agg
JOIN prev_new_users_agg pnu ON ba.tag_app_agg = pnu.tag_app_agg
WHERE ba.tag_app_agg NOT IN ('smartweavecontract', 'smartweaveaction', 'smartweavecontractsource') --AND num_tx_14d > 0
ORDER BY tag_app_agg DESC