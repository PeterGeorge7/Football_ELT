CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.matches_raw (
    source_file TEXT PRIMARY KEY,
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    raw_data JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.teams_raw (
    source_file TEXT PRIMARY KEY,
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    raw_data JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.scorers_raw (
    source_file TEXT PRIMARY KEY,
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    raw_data JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.standings_raw (
    source_file TEXT PRIMARY KEY,
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    raw_data JSONB NOT NULL
);