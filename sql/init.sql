-- ============================================================================
-- Pulse Commerce Engine: Medallion Warehouse Schema (PostgreSQL 17)
-- ============================================================================

-- 1. BRONZE LAYER: Immutable raw scrape audit log
CREATE TABLE IF NOT EXISTS bronze_raw_scrapes (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID        NOT NULL,
    zone_id     TEXT        NOT NULL,
    page_num    INTEGER     NOT NULL,
    xhr_status  SMALLINT,
    payload     JSONB       NOT NULL,
    scraped_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bronze_run_zone
    ON bronze_raw_scrapes (run_id, zone_id);

-- 2. SILVER LAYER (Current State): Deduplicated SKU snapshot per zone
CREATE TABLE IF NOT EXISTS silver_latest_skus (
    zone_id       TEXT           NOT NULL,
    product_url   TEXT           NOT NULL,
    title         TEXT           NOT NULL,
    price         NUMERIC(10, 2) NOT NULL,
    description   TEXT,
    reviews_count INTEGER        NOT NULL DEFAULT 0,
    rating        SMALLINT       NOT NULL DEFAULT 0,
    in_stock      BOOLEAN        NOT NULL DEFAULT TRUE,
    last_run_id   UUID           NOT NULL,
    updated_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (zone_id, product_url)
);

-- 3. SILVER LAYER (Time-Series Ledger): Append-only price observations
CREATE TABLE IF NOT EXISTS silver_price_history (
    id          BIGSERIAL      PRIMARY KEY,
    run_id      UUID           NOT NULL,
    zone_id     TEXT           NOT NULL,
    product_url TEXT           NOT NULL,
    price       NUMERIC(10, 2) NOT NULL,
    in_stock    BOOLEAN        NOT NULL DEFAULT TRUE,
    observed_at TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_history_lookup
    ON silver_price_history (zone_id, product_url, observed_at DESC, id DESC);

-- 4. GOLD LAYER (Analytical View): Price movements via LAG() window function
CREATE OR REPLACE VIEW gold_price_movements AS
WITH ordered_prices AS (
    SELECT
        zone_id,
        product_url,
        price AS current_price,
        LAG(price) OVER (
            PARTITION BY zone_id, product_url
            ORDER BY observed_at ASC, id ASC
        ) AS previous_price,
        in_stock,
        run_id,
        observed_at
    FROM silver_price_history
)
SELECT
    zone_id,
    product_url,
    current_price,
    previous_price,
    (current_price - previous_price) AS price_delta,
    in_stock,
    run_id,
    observed_at
FROM ordered_prices;