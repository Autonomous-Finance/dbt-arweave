{{
    config(
        materialized='incremental',
        order_by='created_at, data_service_id, data_feed_id, id',
        engine='ReplacingMergeTree',
        incremental_strategy='append'
    )
}}

SELECT
    id,
    created_at,
    data_size,
    get_tag('type', tags) AS type,
    get_tag('timestamp', tags) AS timestamp,
    get_tag('dataServiceId', tags) AS data_service_id,
    get_tag('signerAddress', tags) AS signer_address,
    get_tag('dataFeedId', tags) AS data_feed_id,
    get_tag('uploadService', tags) AS upload_service
FROM
    prod.transactions_silver_tbl
WHERE transactions_silver_tbl.agg_content_type = 'Redstone Oracles'

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  AND created_at > (select max(created_at) from {{ this }})

{% endif %}