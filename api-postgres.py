"""Incremental loader: Melbourne microclimate sensor API -> PostgreSQL.

Normalized model:
  - microclimate_device   (master): device_id, sensor location, coordinates
  - microclimate_reading  (child) : one row per device_id + received_at
                                    with all measurement attributes

Watermark table tracks the last loaded received_at so each run only
fetches new records (keyset pagination, no 10k offset limit).
"""

import truststore
truststore.inject_into_ssl()  # must run before any SSL context is created
import sys
import logging
import os
import time
from typing import Any

import psycopg2
import requests
from psycopg2.extras import execute_values
import argparse

API_URL = (
    "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/"
    "datasets/microclimate-sensors-data/records"
)

TABLE_NAME = "microclimate_sensor_data"
DATASET_NAME = "microclimate-sensors-data"
SCHEMA_NAME = "ling_lin"
DEVICE_TABLE = "microclimate_device"
READING_TABLE = "microclimate_reading"
WATERMARK_TABLE = "ingest_watermark"

TIMESTAMP_FIELD = "received_at"

PAGE_SIZE = 100  # Explore v2.1 max per request
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 3

# Measurement attributes that live on the reading (child) table.
# Maps API field name -> DB column name.
MEASUREMENT_FIELDS = {
    "minimumwinddirection": "minimum_wind_direction",
    "averagewinddirection": "average_wind_direction",
    "maximumwinddirection": "maximum_wind_direction",
    "minimumwindspeed": "minimum_wind_speed",
    "averagewindspeed": "average_wind_speed",
    "gustwindspeed": "gust_wind_speed",
    "airtemperature": "air_temperature",
    "relativehumidity": "relative_humidity",
    "atmosphericpressure": "atmospheric_pressure",
    "pm25": "pm25",
    "pm10": "pm10",
    "noise": "noise",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

class ConnectorArgs():
    def __init__(self, args):
        self.parser = argparse.ArgumentParser(description="Extract DB credentials")
        self.parser.add_argument( "--host", help="Host Name", required=True)
        self.parser.add_argument( "--port", help="port", required=True)
        self.parser.add_argument( "--dbname", help="Database Name", required=True)
        self.parser.add_argument("--user", help="User Name", required=True)
        self.parser.add_argument("--password", help="Password", required=True)
        self.parsed_args = self.parser.parse_args(args)

    def get_parsed_args(self):
        return self.parsed_args


def get_database_connection(db_args):
    """Create a PostgreSQL database connection from environment variables."""

    #self.args = ConnectorArgs(args).get_parsed_args()
    return psycopg2.connect(
        host=db_args.host,
        port=db_args.port,
        dbname=db_args.dbname,
        user=db_args.user,
        password=db_args.password,
        connect_timeout=30,
    )


def create_tables(connection) -> None:
    """Create master, child, and watermark tables if they do not exist."""
    reading_columns = ",\n            ".join(
        f"{col} DOUBLE PRECISION" for col in MEASUREMENT_FIELDS.values()
    )

    create_sql = f"""        
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
            row_hash       VARCHAR(64) PRIMARY KEY,
            record_data    JSONB NOT NULL,
            source_dataset VARCHAR(200) NOT NULL,
            ingested_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_record_data
        ON {SCHEMA_NAME}.{TABLE_NAME}
        USING GIN (record_data);

        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_ingested_at
        ON {SCHEMA_NAME}.{TABLE_NAME} (ingested_at);

        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{DEVICE_TABLE} (
            device_id       VARCHAR(100) PRIMARY KEY,
            sensor_location VARCHAR(500),
            latitude        DOUBLE PRECISION,
            longitude       DOUBLE PRECISION,
            first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{READING_TABLE} (
            device_id       VARCHAR(100) NOT NULL
                REFERENCES {SCHEMA_NAME}.{DEVICE_TABLE} (device_id),
            received_at     TIMESTAMPTZ NOT NULL,
            {reading_columns},
            ingested_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (device_id, received_at)
        );

        CREATE INDEX IF NOT EXISTS idx_{READING_TABLE}_received_at
        ON {SCHEMA_NAME}.{READING_TABLE} (received_at);

        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{WATERMARK_TABLE} (
            dataset_name   VARCHAR(200) PRIMARY KEY,
            last_timestamp TIMESTAMPTZ,
            updated_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """
    with connection.cursor() as cursor:
        cursor.execute(create_sql)
    connection.commit()


def get_watermark(connection) -> str | None:
    """Return the last loaded timestamp for this dataset, or None."""
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT last_timestamp
            FROM {SCHEMA_NAME}.{WATERMARK_TABLE}
            WHERE dataset_name = %s
            """,
            (DATASET_NAME,),
        )
        row = cursor.fetchone()
    if row and row[0]:
        return row[0].isoformat()
    return None


def update_watermark(cursor, last_timestamp: str) -> None:
    """Upsert the watermark. Runs in the caller's transaction."""
    cursor.execute(
        f"""
        INSERT INTO {SCHEMA_NAME}.{WATERMARK_TABLE}
            (dataset_name, last_timestamp, updated_at)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (dataset_name) DO UPDATE
        SET last_timestamp = EXCLUDED.last_timestamp,
            updated_at     = CURRENT_TIMESTAMP
        """,
        (DATASET_NAME, last_timestamp),
    )


def call_api(params: dict[str, Any]) -> dict[str, Any]:
    """GET with retry/backoff on 429 rate limiting."""
    for attempt in range(MAX_RETRIES + 1):
        response = requests.get(
            API_URL,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
            headers={
                "Accept": "application/json",
                "User-Agent": "microclimate-postgres-loader/1.0",
            },
        )
        if response.status_code == 429 and attempt < MAX_RETRIES:
            wait = int(response.headers.get("Retry-After", 2**attempt * 5))
            logger.warning(
                "Rate limited (attempt %s/%s); sleeping %ss",
                attempt + 1,
                MAX_RETRIES,
                wait,
            )
            time.sleep(wait)
            continue
        response.raise_for_status()
        return response.json()
    raise RuntimeError("Exhausted retries calling API")


def upsert_devices(cursor, records: list[dict[str, Any]]) -> None:
    """Upsert master rows: one per distinct device in the batch.

    Location/coordinates update on conflict so a relocated device
    reflects its latest position.
    """
    devices: dict[str, tuple] = {}
    for record in records:
        device_id = record.get("device_id")
        if not device_id:
            continue
        latlong = record.get("latlong") or {}
        devices[device_id] = (
            device_id,
            record.get("sensorlocation"),
            latlong.get("lat"),
            latlong.get("lon"),
        )

    if not devices:
        return

    execute_values(
        cursor,
        f"""
        INSERT INTO {SCHEMA_NAME}.{DEVICE_TABLE}
            (device_id, sensor_location, latitude, longitude)
        VALUES %s
        ON CONFLICT (device_id) DO UPDATE
        SET sensor_location = EXCLUDED.sensor_location,
            latitude        = EXCLUDED.latitude,
            longitude       = EXCLUDED.longitude,
            updated_at      = CURRENT_TIMESTAMP
        WHERE ({SCHEMA_NAME}.{DEVICE_TABLE}.sensor_location,
               {SCHEMA_NAME}.{DEVICE_TABLE}.latitude,
               {SCHEMA_NAME}.{DEVICE_TABLE}.longitude)
              IS DISTINCT FROM
              (EXCLUDED.sensor_location,
               EXCLUDED.latitude,
               EXCLUDED.longitude)
        """,
        list(devices.values()),
        template="(%s, %s, %s, %s)",
    )


def insert_readings(cursor, records: list[dict[str, Any]]) -> int:
    """Insert child rows, skipping (device_id, received_at) duplicates."""
    rows = []
    for record in records:
        device_id = record.get("device_id")
        received_at = record.get(TIMESTAMP_FIELD)
        if not device_id or not received_at:
            logger.warning("Skipping record missing device_id/received_at")
            continue
        rows.append(
            (device_id, received_at)
            + tuple(record.get(field) for field in MEASUREMENT_FIELDS)
        )

    if not rows:
        return 0

    columns = ["device_id", "received_at"] + list(MEASUREMENT_FIELDS.values())
    placeholders = ", ".join(["%s"] * len(columns))

    inserted = execute_values(
        cursor,
        f"""
        INSERT INTO {SCHEMA_NAME}.{READING_TABLE} ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (device_id, received_at) DO NOTHING
        RETURNING device_id
        """,
        rows,
        template=f"({placeholders})",
        page_size=PAGE_SIZE,
        fetch=True,
    )
    return len(inserted)

def insert_records_json(
    connection,
    records: list[dict[str, Any]],
) -> int:
    """Insert a batch and advance the watermark in ONE transaction.

    Committing both together means a crash can never leave the
    watermark ahead of the data actually stored.
    """
    if not records:
        return 0

    rows = [
        (generate_row_hash(record), Json(record), DATASET_NAME)
        for record in records
    ]

    insert_sql = f"""
        INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
            row_hash,
            record_data,
            source_dataset
        )
        VALUES %s
        ON CONFLICT (row_hash) DO NOTHING
        RETURNING row_hash;
    """

    with connection.cursor() as cursor:
        inserted = execute_values(
            cursor,
            insert_sql,
            rows,
            template="(%s, %s, %s)",
            page_size=PAGE_SIZE,
            fetch=True,
        )        

    connection.commit()
    return len(inserted)


def process_batch(
    connection,
    records: list[dict[str, Any]],
    last_timestamp: str,
) -> int:
    """Upsert devices, insert readings, advance watermark - one transaction."""
    with connection.cursor() as cursor:
        insert_records_json(cursor, records)
        upsert_devices(cursor, records)
        inserted = insert_readings(cursor, records)
        update_watermark(cursor, last_timestamp)
    connection.commit()
    return inserted


def load_new_records(connection) -> None:
    """Fetch records newer than the watermark using keyset pagination."""
    watermark = get_watermark(connection)
    logger.info("Starting incremental load. Watermark=%s", watermark)

    total_received = 0
    total_inserted = 0
    tie_offset = 0

    while True:
        params: dict[str, Any] = {
            "limit": PAGE_SIZE,
            "offset": tie_offset,
            "order_by": f"{TIMESTAMP_FIELD} asc",
        }
        if watermark:
            params["where"] = f"{TIMESTAMP_FIELD} >= date'{watermark}'"

        logger.info(
            "Requesting records: where>=%s, offset=%s", watermark, tie_offset
        )
        payload = call_api(params)
        records = payload.get("results", [])

        if not isinstance(records, list):
            raise ValueError("Unexpected API response: 'results' is not a list")

        if not records:
            break

        page_last_ts = records[-1].get(TIMESTAMP_FIELD)
        if page_last_ts is None:
            raise ValueError(
                f"Field '{TIMESTAMP_FIELD}' missing from API record"
            )

        inserted_count = process_batch(connection, records, page_last_ts)

        total_received += len(records)
        total_inserted += inserted_count
        logger.info(
            "Received=%s, inserted=%s (page last ts=%s)",
            total_received,
            total_inserted,
            page_last_ts,
        )

        if len(records) < PAGE_SIZE:
            break

        if page_last_ts == watermark:
            # Entire page shares the watermark timestamp; page through
            # the tie with a small offset.
            tie_offset += len(records)
        else:
            watermark = page_last_ts
            tie_offset = 0

    logger.info(
        "Load completed. Received %s records, inserted %s new readings.",
        total_received,
        total_inserted,
    )


def main() -> int:
    connection = None
    try:
        connection = get_database_connection(db_args)
        create_tables(connection)
        load_new_records(connection)
        return 0
    except Exception:
        logger.exception("Microclimate incremental load failed")
        return 1
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    args = sys.argv[1:]
    db_args = ConnectorArgs(args).get_parsed_args()
    raise SystemExit(main())