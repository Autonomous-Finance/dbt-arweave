{{
    config(
        materialized='incremental',
        order_by='created_at, id',
        engine='ReplacingMergeTree',
        incremental_strategy='append'
    )
}}

WITH tags_extracted AS (
    SELECT
        id,
        owner,
        tags,
        target,
        quantity,
        reward,
        signature,
        last_tx,
        data_size,
        content_type,
        format,
        created_at,
        deleted_at,
        height,
        owner_address,
        data_root,
        parent,
        is_deleted,
        inserted_at,
        replaceAll(lower(arrayFilter(t -> lower(t['name']) = 'content-type', tags)[1]['value']), '; charset=utf-8', '') AS tag_content_type,
        get_app_tag(tags)[1] AS tag_app,
        lower(arrayFilter(t -> lower(t['name']) = 'type', tags)[1]['value']) AS tag_type,
        length(arrayFilter(t -> lower(t['name']) = 'bundle-format', tags)) > 0 AS is_bundle,
        length(arrayFilter(t -> lower(t['value']) = 'smartweaveaction', tags)) > 0 AS is_smartweave_action,
        length(arrayFilter(t -> lower(t['value']) IN ('smartweavecontract', 'smartweavecontractsource', 'smartweavecontractsource'), tags)) > 0 AS is_smartweave_contract
    FROM {{ source('goldsky', 'transactions') }}

    {% if env_var('DBT_SCHEMA') != 'prod' %}
    LIMIT 1000000
    {% endif %}
),
split_content_types AS (
    SELECT
        *,
        trim(splitByChar('/', tag_content_type)[1]) AS tag_main_content_type,
        trim(splitByChar('/', tag_content_type)[2]) AS tag_sub_content_type
    FROM tags_extracted
), agg_content_type AS (
    SELECT
        *,
        CASE
          WHEN tag_type LIKE 'redstone-oracles' OR owner_address = 'I-5rWUehEv-MjdK9gFw09RxfSLQX9DIHxG614Wf8qo0' THEN 'Redstone Oracles'
          WHEN tag_content_type LIKE 'encrypted%' THEN 'Encrypted'
          WHEN is_smartweave_contract OR is_smartweave_action THEN 'Smartweave'
          WHEN (trim(target) != '' AND data_size = 0 AND (toFloat64OrZero(quantity) / 1e12) > 0) OR tag_type = 'fee-transaction' THEN 'Transaction'
          WHEN tag_content_type = 'application/json' THEN 'JSON'
          WHEN tag_main_content_type IN ('application', 'image', 'text', 'video', 'model', 'json', 'audio') THEN INITCAP(tag_main_content_type)
          WHEN get_tag('Data-Protocol', tags) = 'ao' THEN 'AO'
          WHEN tag_main_content_type = '' THEN 'Undefined Content Type'
          ELSE 'Other'
        END AS agg_content_type,
        CASE
            WHEN tag_app LIKE 'sugar%' THEN 'sugar'
            WHEN tag_app LIKE 'ardrive-%' THEN 'ardrive'
            ELSE tag_app
        END AS tag_app_agg
    FROM split_content_types
)
SELECT
    ac.* EXCEPT ( created_at, height, quantity, reward, inserted_at ),
    ac.inserted_at,
    toFloat64OrZero(quantity) / 1e12 AS quantity,
    toFloat64OrZero(reward) / 1e12 AS reward,
    coalesce(height, 0) AS height,
    ac.created_at AS created_at_goldsky,
    toDateTime(bd.timestamp) AS created_at_gql,
    CASE WHEN timestamp != 0 THEN toDateTime(timestamp) ELSE created_at END AS created_at
FROM agg_content_type ac
LEFT JOIN {{ source('gateway', 'block_dates') }} bd FINAL on ac.height = bd.height

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  WHERE inserted_at > (select max(inserted_at) from {{ this }})

{% endif %}