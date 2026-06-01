-- Session settings for the CDC upsert/dedup job.
-- Loaded as an sql-client init script (-i), so these apply to the whole session.

-- Streaming, with exactly-once checkpointing every 10s.
-- Checkpoints are what make the upsert/Iceberg sink exactly-once (Step 5).
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- Keep state for unbounded keys bounded-ish; tune in production.
SET 'parallelism.default' = '2';

-- Submit INSERTs as detached streaming jobs (don't block the client).
SET 'table.dml-sync' = 'false';
