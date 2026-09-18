# Staging Database Contract

## Purpose

The staging layer stores the latest raw API response received
from football-data.org before dbt transformation.

## Schema

PostgreSQL schema:

`staging`

## Tables

- `matches_raw`
- `teams_raw`
- `scorers_raw`
- `standings_raw`

## Grain

Each row represents one source file / one API snapshot.

## Columns

| Column | Type | Constraint | Description |
|---|---|---|---|
| source_file | TEXT | PRIMARY KEY | Unique MinIO object path representing the source |
| ingestion_timestamp | TIMESTAMPTZ | NOT NULL | Time the API response was loaded |
| raw_data | JSONB | NOT NULL | Complete API response |

## Rerun behavior

The load is idempotent.

If `source_file` does not exist:
- INSERT a new row.

If `source_file` already exists:
- UPDATE `ingestion_timestamp`
- UPDATE `raw_data`

Therefore, rerunning the same ingestion does not create duplicate rows.

## History

Version 1 keeps only the latest response for each `source_file`.
Historical API responses are not retained.

## Relationship with Bronze

The Bronze MinIO object uses the same logical source identifier.
The current implementation uses `replace=True`, so the latest object
replaces the previous object.