# Microclimate Sensor Data Loader

Incremental ETL script that fetches Melbourne microclimate sensor readings from
the [City of Melbourne Open Data API](https://data.melbourne.vic.gov.au/) and
loads them into PostgreSQL.

Source dataset: `microclimate-sensors-data`
API endpoint: `https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/microclimate-sensors-data/records`

## What it does

Each run:

1. Ensures the target tables exist (`CREATE TABLE IF NOT EXISTS`).
2. Reads a **watermark** (the `received_at` of the last successfully loaded
   record) from `ingest_watermark`.
3. Pages through the API in ascending `received_at` order, requesting only
   records at or after the watermark (keyset pagination — no reliance on the
   API's offset limit, so it scales past 10k rows).
4. For each page, in a single transaction:
   - Upserts the raw JSON payload into an audit table (`microclimate_sensor_data`).
   - Upserts device master rows (`microclimate_device`).
   - Inserts reading rows (`microclimate_reading`), skipping duplicates.
   - Advances the watermark.
5. Repeats until a page comes back shorter than the page size (i.e. no more
   data), handling same-timestamp ties via a small offset so no records are
   silently dropped when many rows share the same `received_at`.

Because the watermark update is committed in the same transaction as the
data insert, a crash mid-run can never leave the watermark ahead of what's
actually stored — the next run will safely re-fetch and no-op on
already-inserted rows.

## Schema

All tables live under the `ling_lin` schema.

| Table | Purpose |
|---|---|
| `microclimate_sensor_data` | Raw audit copy of every fetched record, keyed by a content hash (`row_hash`), stored as `JSONB` with a GIN index for ad-hoc querying. |
| `microclimate_device` | Master table, one row per `device_id`, with sensor location and lat/lon. Updated on conflict if a device's location changes. |
| `microclimate_reading` | Child table, one row per `(device_id, received_at)`, with all measurement columns (wind, temperature, humidity, pressure, PM2.5/PM10, noise). |
| `ingest_watermark` | Tracks the last loaded `received_at` per dataset so re-runs are incremental. |

Measurement fields pulled from the API and their DB column names are defined
in `MEASUREMENT_FIELDS` in the script.

## Requirements

- Python 3.10+ (uses the `X | None` type union syntax)
- PostgreSQL (schema `ling_lin` must exist, or be creatable by the DB user)

Python packages:

```
pip install psycopg2-binary requests truststore
```

## Usage

Database credentials are passed as required CLI arguments (no environment
variables or config file):

```
python api-postgres.py \
  --host <db-host> \
  --port <db-port> \
  --dbname <db-name> \
  --user <db-user> \
  --password <db-password>
```

Run it again at any time (e.g. on a schedule) — each run only fetches
records newer than the last watermark.

Example call:

```
python api-postgres_2.py \
  --host ds-ods-dev2.dataservices.awsnonprod.internal \
  --port 5432 \
  --dbname ods_db \
  --user ods_etl_usr \
  --password <password>
```

## Execution flow

`main()` is the entry point and drives everything else in this order:

```
main()
├── ConnectorArgs(...).get_parsed_args()   # parse --host/--port/--dbname/--user/--password
├── get_database_connection(db_args)       # open the psycopg2 connection
├── create_tables(connection)              # CREATE TABLE IF NOT EXISTS (device/reading/audit/watermark)
└── load_new_records(connection)           # main incremental load loop
    ├── get_watermark(connection)          # read last loaded received_at
    └── loop until a short page is returned:
        ├── call_api(params)               # GET the next page, retries on HTTP 429
        └── process_batch(connection, records, page_last_ts)   # one DB transaction per page
            ├── insert_records_json(cursor, records)  # raw JSONB audit insert
            ├── upsert_devices(cursor, records)       # upsert microclimate_device
            ├── insert_readings(cursor, records)      # insert microclimate_reading, skip dupes
            └── update_watermark(cursor, page_last_ts)  # advance ingest_watermark
```

Each iteration of the loop in `load_new_records` commits one page's worth of
work — records, devices, readings, and the watermark — as a single
transaction via `process_batch`, before requesting the next page.

## Notes

- `truststore.inject_into_ssl()` is called at import time (before any SSL
  context is created) so `requests` uses the OS trust store — useful in
  environments with corporate TLS-inspecting proxies.
- API calls retry on HTTP 429 with exponential backoff (or the
  `Retry-After` header if present), up to `MAX_RETRIES` (3) attempts.
- Page size is capped at 100, the Explore v2.1 API's per-request maximum.

## Query used to generate sample CSV

select d.device_id , d.sensor_location , d.latitude , d.longitude, r.received_at ,
r.air_temperature ,r.atmospheric_pressure ,r.average_wind_speed ,r.relative_humidity ,r.maximum_wind_direction , r.noise ,r.pm10
from ling_lin.microclimate_device d
join ling_lin.microclimate_reading r
on d.device_id = r.device_id
where d.device_id = 'ICTMicroclimate-01'
and r.received_at::date = '2022-06-02'
order by r.received_at ;

