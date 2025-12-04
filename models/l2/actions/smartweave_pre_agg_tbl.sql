{{
    config(
        materialized='incremental',
        engine='MergeTree',
        order_by='contract, created_at',
        incremental_strategy='append'
    )
}}

SELECT
    id,
    created_at,
    owner,
    owner_address,
    coalesce(MAX(CASE WHEN l_name = 'contract' THEN value END), 'NO_CONTRACT') AS contract,
    coalesce(MAX(CASE WHEN l_name = 'input' THEN JSONExtractString(l_value, 'function') END), 'NO_FUNCTION') AS function,
    MAX(CASE WHEN l_name = 'input' THEN l_value END) AS input,
    MAX(CASE WHEN l_name = 'signing-client' THEN l_value END) AS signing_client,
    MAX(CASE WHEN l_name = 'client-name' THEN l_value END) AS client_name,
    MAX(CASE WHEN l_name = 'app-name' THEN l_value END) AS app_name,
    MAX(CASE WHEN l_name = 'protocol-name' THEN l_value END) AS protocol_name,
    MAX(CASE WHEN l_name = 'action' THEN l_value END) AS action,
    arrayFilter(
        item -> item['name'] != 'REMOVE_ME',
        groupArray(CASE WHEN l_name NOT IN ('contract',
                                        'input',
                                        'signing-client',
                                        'client-name',
                                        'app-name',
                                        'protocol-name',
                                       'action')
                        THEN t
                    ELSE map('name', 'REMOVE_ME') END)) AS rest_tags
FROM {{ref('smartweave_tags_mv')}}
WHERE
    NOT (
            (l_name = 'app-name' AND l_value = 'smartweaveaction') OR
            (l_name = 'app-version' AND l_value = '0.3.0')
        )
    {% if is_incremental() %}
    AND created_at > (select max(created_at) from {{ this }})
    {% endif %}
GROUP BY id, created_at, owner, owner_address
