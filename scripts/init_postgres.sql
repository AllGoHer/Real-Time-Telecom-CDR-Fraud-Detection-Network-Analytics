-- Tabla para almacenar métricas de fraude en tiempo real
CREATE TABLE IF NOT EXISTS fraud_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    country_code VARCHAR(2),
    total_calls INT,
    suspicious_calls INT,
    fraud_percentage DECIMAL(5,2),
    avg_duration_sec DECIMAL(10,2),
    top_suspicious_caller VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla para almacenar alertas de fraude individuales
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id SERIAL PRIMARY KEY,
    call_id VARCHAR(50) NOT NULL,
    caller VARCHAR(20) NOT NULL,
    callee VARCHAR(20) NOT NULL,
    duration_sec INT,
    country_code VARCHAR(2),
    timestamp TIMESTAMP NOT NULL,
    fraud_score DECIMAL(5,2),
    fraud_reason VARCHAR(100),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para mejorar rendimiento de consultas
CREATE INDEX IF NOT EXISTS idx_fraud_metrics_window ON fraud_metrics(window_start);
CREATE INDEX IF NOT EXISTS idx_fraud_metrics_country ON fraud_metrics(country_code);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_caller ON fraud_alerts(caller);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_timestamp ON fraud_alerts(timestamp);