with feed_first_seen AS (
    SELECT data_feed_id, min(created_date) as first_seen_dt
    FROM {{ ref('redstone_daily_detailed') }}
    GROUP BY data_feed_id
), daily_new_data_feeds AS (
    SELECT
        first_seen_dt,
        count(1) as daily_new_protocols
    FROM feed_first_seen
    GROUP BY first_seen_dt
)
SELECT
    created_date,
    sum(transactions) over (order by created_date) AS transactions,
    sum(data_size) over (order by created_date) AS data_size,
    sum(daily_new_protocols) over (order by created_date) as data_feeds
FROM {{ ref('redstone_daily_agg') }} as rd
LEFT JOIN daily_new_data_feeds ffs
    ON ffs.first_seen_dt = rd.created_date