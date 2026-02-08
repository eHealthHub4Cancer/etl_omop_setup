# OMOP ETL Setup (Postgres)

This repo automates the “first day” tasks for standing up an OMOP CDM database in Postgres: schema creation, roles/users, DDL, optional CSV loads, and optional Achilles scripts. The entry point is `main.py`.

## What’s in here

- `main.py` orchestrates the setup flow (connect → roles → DDL → optional data load → optional Achilles).
- `code_base/` holds the database connector, DDL runner, and CSV loader.
- `5.4/` contains OMOP CDM 5.4 DDL, indices, and constraints SQL files.
- `achilles_scripts/` contains the SQL files for Achilles results and counts.
- `database/` contains a `docker-compose.yml` for a local Postgres instance.

## Prerequisites

- Python 3.10+ (3.11 is fine).
- A Postgres instance you can administer **or** Docker for the included database container.
- OMOP CDM SQL and data files on disk (the repo already includes the v5.4 SQL files).

## Quick start (local Postgres via Docker)

If you want a local Postgres you can wipe easily, use the compose file in `database/`.

```bash
cd database
DB_NAME=omop DB_USER=omop_admin DB_PASSWORD=omop_pass DB_PORT=5432 docker compose up -d
```

That gives you a database you can point the Python script at. The Python script uses **its own** environment variables (see next section), so don’t forget to set both.

## Configuration

The setup script reads everything from environment variables via `python-dotenv`. You can export them directly or create a `.env` file in the repo root.

### Required environment variables

| Variable | Purpose |
| --- | --- |
| `dbUser` / `dbPassword` / `dbHost` / `dbPort` / `dbName` | Primary admin connection used for schema/role setup. |
| `cdmDatabaseSchema` | Schema for OMOP CDM tables. |
| `vocabDatabaseSchema` | Schema for vocabulary tables. |
| `resultSchema` | Schema for results. |
| `scratchSchema` | Schema for scratch work. |
| `webApiSchema` | Schema for WebAPI tables. |
| `tempSchema` | Schema for temp tables. |
| `dbUser1` / `dbPassword1` | ETL user credentials. |
| `dbUser2` / `dbPassword2` | WebAPI user credentials. |
| `role1` | Role name for ETL. |
| `role2` | Role name for WebAPI. |
| `vocabDdl` / `cdmDdl` | Paths to vocab/CDM DDL SQL files. |
| `vocabPrimaryKeys` / `cdmPrimaryKeys` | Paths to primary key SQL files. |
| `vocabIndices` / `cdmIndices` | Paths to indices SQL files. |
| `constraints` | Path to constraints SQL file. |
| `vocabCsvFolder` / `cdmCsvFolder` | Folder paths to vocabulary/CDM CSVs (if loading). |
| `achillesResult` / `achillesCount` | Paths to Achilles SQL files (optional). |

### Example `.env`

```ini
# Database connection (admin)
dbUser=omop_admin
dbPassword=omop_pass
dbHost=localhost
dbPort=5432
dbName=omop

# Schemas
cdmDatabaseSchema=cdm
tempSchema=temp
vocabDatabaseSchema=vocab
resultSchema=results
scratchSchema=scratch
webApiSchema=webapi

# Users and roles
role1=etl_role
role2=webapi_role
dbUser1=etl_user
dbPassword1=etl_pass
dbUser2=webapi_user
dbPassword2=webapi_pass

# SQL file paths (CDM 5.4 in this repo)
cdmDdl=5.4/cdm.sql
vocabDdl=5.4/vocab.sql
cdmPrimaryKeys=5.4/cdm_primary_keys.sql
vocabPrimaryKeys=5.4/vocab_primary_keys.sql
cdmIndices=5.4/cdm_indices.sql
vocabIndices=5.4/vocab_indices.sql
constraints=5.4/constraints.sql

# CSV folders (only needed if you load data)
cdmCsvFolder=/absolute/path/to/cdm/csvs
vocabCsvFolder=/absolute/path/to/vocab/csvs

# Achilles (optional)
achillesResult=achilles_scripts/achilles_result.sql
achillesCount=achilles_scripts/achilles_count.sql

# View for Cohort Diagnostic Analysis, Feature Extraction, and more
cdmSchemas=cdm_schema1,cdm_schema2
#vocab tables for views seperated by comma.
vocabTables=concept,concept_ancestor,concept_relationship,concept_synonym,domain,relationship,source_to_concept_map,vocabulary,drug_strength,concept_class

```

## Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the setup

By default, `main.py` runs:

1. Database connection
2. Role and schema setup
3. DDL + primary keys + indices

The CSV load and Achilles steps are intentionally commented out. That lets you control when data load and analysis happen.

```bash
python main.py
```

### Loading CSV data

If you want to load CDM or vocabulary CSVs, set `cdmCsvFolder` and/or `vocabCsvFolder`, then uncomment the `load_initial_data(...)` call in `main.py`.

### Running Achilles

Achilles can be heavy; the script keeps it off by default. Set `achillesResult` and `achillesCount`, then uncomment `run_achilles_analysis(...)` in `main.py`. The comment block in the script explains the expected order.

## Notes

- The DDL runner replaces `@cdmDatabaseSchema`, `@vocabDatabaseSchema`, `@resultSchema`, and `@tempSchema` placeholders in the SQL files.
- The CSV loader expects **headers** and streams data in chunks.
- If you run the setup more than once, role/user creation will be skipped if they already exist.

## Troubleshooting

- **Connection failures**: double-check `dbHost`, `dbPort`, and credentials, and make sure Postgres is listening.
- **Missing tables during CSV load**: ensure the DDL step completed and you used the correct schema names.
- **Permissions issues**: the admin user you supply should be allowed to create roles and schemas.
