-- Multi-tenant: separate sales data per client (Supabase Auth UID).
-- Run this in Supabase BEFORE deploying the pipeline changes.

ALTER TABLE clearpath.sales ADD COLUMN IF NOT EXISTS client_id uuid;

-- Speed up per-client filtering (DELETE WHERE client_id = ... and mart WHEREs).
CREATE INDEX IF NOT EXISTS idx_sales_client_id ON clearpath.sales(client_id);
