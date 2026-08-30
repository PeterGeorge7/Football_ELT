import os
from datetime import datetime, timezone
from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
import requests
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

base_url = "http://api.football-data.org/v4/"

HEADERS = {"X-Auth-Token": os.getenv("X-Auth-Token")}

BUCKET_NAME = "bronze"


@dag(start_date=datetime(2026, 1, 1), schedule=None, catchup=False)
def elt_football_v1():

    @task.sensor(poke_interval=30, timeout=600, mode="reschedule")
    def check_api_available() -> PokeReturnValue:
        response = requests.get(f"{base_url}competitions", headers=HEADERS)
        return PokeReturnValue(is_done=response.status_code == 200)

    @task
    def get_data(topic: str, season: str):
        url = f"{base_url}{topic}"
        params = {"season": season, "limit": 500}
        start, league, final = topic.split("/")
        s3_key = f"{league}/{season}/{league}_{final}_{season}.json"

        response = requests.get(url, headers=HEADERS, params=params, timeout=30)

        print(f"Request URL: {response.url}")
        print(f"Status code: {response.status_code}")
        print(f"Content-Type: {response.headers.get('Content-Type')}")

        response.raise_for_status()
        data = response.json()

        S3Hook(aws_conn_id="minio_conn").load_string(
            string_data=json.dumps(data),
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )

        print(
            f"Successfully uploaded {s3_key} to S3-compatible bucket '{BUCKET_NAME}'."
        )
        return {
            "bucket": "bronze",
            "key": s3_key,
            "resource": final,
            "league": league,
            "season": season,
        }

    @task
    def staging_loader(metadata):
        connection = S3Hook(aws_conn_id="minio_conn")
        file_content = connection.read_key(
            bucket_name=metadata["bucket"], key=metadata["key"]
        )
        allowed_resources = {
            "standings",
            "matches",
            "scorers",
            "teams",
        }

        resource = metadata["resource"]

        if resource not in allowed_resources:
            raise ValueError(f"Invalid resource: {resource}")

        table_name = f"staging.{resource}_raw"
        ingestion_timestamp = datetime.now(timezone.utc)
        hook = PostgresHook(postgres_conn_id="app_db_conn")
        hook.run(
            f"""
                INSERT INTO {table_name} (
                source_file,
                ingestion_timestamp,
                raw_data
                )
                VALUES (
                    %s,
                    %s,
                    CAST(%s AS JSONB)
                )
                ON CONFLICT (source_file) 
                DO UPDATE SET 
                ingestion_timestamp = EXCLUDED.ingestion_timestamp,
                raw_data = EXCLUDED.raw_data;
                """,
            parameters=(
                metadata["key"],
                ingestion_timestamp,
                file_content,
            ),
        )

        print(f"Connection successful! Test result: 1 row inserted")

    pl_topics = [
        "competitions/PL/standings",
        "competitions/PL/matches",
        "competitions/PL/scorers",
        "competitions/PL/teams",
    ]

    check = check_api_available()

    metadata = get_data.expand(
        topic=pl_topics,
        season=["2023", "2024", "2025"],
    )

    staging_load = staging_loader.expand(metadata=metadata)

    check >> metadata >> staging_load


elt_football_v1()
