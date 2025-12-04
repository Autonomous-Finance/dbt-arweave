-- Index definitions for improved query performance on the transactions table

-- Add bloom filter index on transaction ID for faster lookups
ALTER TABLE prod.transactions_silver_tbl ADD INDEX id_idx id TYPE bloom_filter GRANULARITY 1;
ALTER TABLE prod.transactions_silver_tbl MATERIALIZE INDEX id_idx;

-- Optimize table to apply pending mutations and compact data
OPTIMIZE TABLE prod.transactions_silver_tbl FINAL;
