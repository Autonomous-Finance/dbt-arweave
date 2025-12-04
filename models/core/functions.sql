-- Custom ClickHouse functions for extracting and processing Arweave transaction tags

-- Extract all application-related tag values from a transaction's tags array
CREATE OR REPLACE FUNCTION all_app_tags AS (tags) -> arrayMap(
        app_tag_struct -> lower(app_tag_struct['value']),
        arrayFilter(t -> lower(t['name']) in
                         ('custom-app-name', 'application', 'app-name', 'appname', 'protocol',
                          'protocol-name', 'app-type', 'application-id'), tags)
    );

-- Get the primary app tag, filtering out SmartWeave-related values
CREATE OR REPLACE FUNCTION get_app_tag AS (tags) -> arrayFilter(app_tag -> app_tag NOT IN ('smartweaveaction', 'smartweavecontract', ''), all_app_tags(tags));

-- Extract a specific tag value by name from the tags array
CREATE OR REPLACE FUNCTION get_tag AS (needle_tag, tags) -> arrayFilter(tag -> tag['name'] = needle_tag, tags)[1]['value'];
