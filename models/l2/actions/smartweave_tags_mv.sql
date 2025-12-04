{{
    config(
        materialized='incremental',
        engine='MergeTree',
        order_by='owner_address, created_at',
        incremental_strategy='append'
    )
}}

WITH smartweave_actions AS (
    SELECT
        id,
        created_at,
        owner,
        owner_address,
        arrayJoin(tags) as t
    FROM {{ref('transactions_silver_tbl')}}
    WHERE is_smartweave_action
    {% if is_incremental() %}
    AND created_at > (select max(created_at) from {{ this }})
    {% endif %}
)
SELECT
    id,
    created_at,
    owner,
    owner_address,
    t,
    lower(t['name']) AS l_name,
    lower(t['value']) as l_value,
    t['name'] AS name,
    t['value'] as  value
FROM smartweave_actions