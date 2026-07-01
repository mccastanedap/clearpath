CREATE TABLE IF NOT EXISTS clearpath.weekly_history (
    id bigserial PRIMARY KEY,
    client_id uuid NOT NULL,
    week_start date NOT NULL,
    week_end date NOT NULL,
    total_revenue numeric NOT NULL,
    total_units integer NOT NULL,
    top_product_name text,
    top_product_units integer,
    top_product_revenue numeric,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_weekly_history_client_week UNIQUE (client_id, week_start, week_end)
);

CREATE INDEX IF NOT EXISTS idx_weekly_history_client
    ON clearpath.weekly_history(client_id, week_start);
