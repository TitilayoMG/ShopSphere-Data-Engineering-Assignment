CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.pipeline_runs (
    run_id BIGSERIAL PRIMARY KEY,
    pipeline_name VARCHAR(120) NOT NULL,
    source_name VARCHAR(120) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    status VARCHAR(30) NOT NULL CHECK (status IN ('started', 'success', 'failed', 'skipped')),
    records_extracted INTEGER NOT NULL DEFAULT 0,
    records_loaded INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    watermark_value TEXT
);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_name_source_started ON control.pipeline_runs (pipeline_name, source_name, started_at DESC);

CREATE TABLE IF NOT EXISTS control.pipeline_watermarks (
    pipeline_name VARCHAR(120) NOT NULL,
    source_name VARCHAR(120) NOT NULL,
    watermark_column VARCHAR(120) NOT NULL,
    watermark_value TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_name, source_name)
);

COMMENT ON SCHEMA staging IS 'Landing schema for tables loaded from processed lake files.';
COMMENT ON SCHEMA analytics IS 'Mentee-created dimensional and fact tables for reporting.';
COMMENT ON SCHEMA control IS 'Pipeline run history and incremental-load watermarks.';
