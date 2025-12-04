{{
    config(
        materialized='table',
        engine='MergeTree',
    )
}}
WITH m_agg AS (
    SELECT
        toStartOfMonth(created_at) as created_date,
        sum(smartweave_actions) AS smartweave_actions,
        sum(smartweave_contracts) AS smartweave_contracts
    FROM {{ ref('l2_base_v2') }}
    GROUP BY 1
)
SELECT
    created_date,
    SUM(smartweave_actions) OVER (ORDER BY created_date) AS rolling_l2_actions,
    SUM(smartweave_contracts) OVER (ORDER BY created_date) AS rolling_l2_contracts
FROM m_agg
