CREATE TABLE IF NOT EXISTS taxpayer_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_income DOUBLE PRECISION,
    filings_count INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

