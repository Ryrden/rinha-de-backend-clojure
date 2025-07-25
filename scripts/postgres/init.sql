CREATE TABLE IF NOT EXISTS payments (
    correlation_id UUID PRIMARY KEY,
    amount DECIMAL(15,2) NOT NULL,
    requested_at TIMESTAMP NOT NULL,
    processor VARCHAR(10) NOT NULL CHECK (processor IN ('default', 'fallback'))
);

CREATE INDEX IF NOT EXISTS idx_payments_correlation_id ON payments(correlation_id);
CREATE INDEX IF NOT EXISTS idx_payments_requested_at ON payments(requested_at);
CREATE INDEX IF NOT EXISTS idx_payments_processor ON payments(processor);
