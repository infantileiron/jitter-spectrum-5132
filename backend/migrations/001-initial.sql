-- Initial demo schema comes from versioned SQLAlchemy metadata.
-- DOUBLE time/frequency parameters; JSON retains original points, no imputation.
-- Unique dataset/window/study names and report.run_id. Production requires reviewed ALTER migrations.
CREATE INDEX IF NOT EXISTS ix_spectral_runs_status ON spectral_runs(status);
-- Existing deployments: add run lifecycle columns before starting the new worker.
ALTER TABLE spectral_runs ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 0;
ALTER TABLE spectral_runs ADD COLUMN IF NOT EXISTS owner VARCHAR NOT NULL DEFAULT '';
ALTER TABLE spectral_runs ADD COLUMN IF NOT EXISTS lease_expires_at VARCHAR NOT NULL DEFAULT '';
ALTER TABLE spectral_runs ADD COLUMN IF NOT EXISTS heartbeat_at VARCHAR NOT NULL DEFAULT '';
ALTER TABLE spectral_runs ADD COLUMN IF NOT EXISTS started_at VARCHAR NOT NULL DEFAULT '';
ALTER TABLE spectral_runs ADD COLUMN IF NOT EXISTS finished_at VARCHAR NOT NULL DEFAULT '';
ALTER TABLE spectral_runs ADD COLUMN IF NOT EXISTS canceled_at VARCHAR NOT NULL DEFAULT '';
ALTER TABLE spectral_runs ADD COLUMN IF NOT EXISTS canceled_by VARCHAR NOT NULL DEFAULT '';
ALTER TABLE spectral_runs ADD COLUMN IF NOT EXISTS retry_of VARCHAR NOT NULL DEFAULT '';
