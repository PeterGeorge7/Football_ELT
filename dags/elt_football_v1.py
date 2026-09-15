import os
from datetime import datetime, timezone
from time import sleep
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

    # sensor to check availability of the api and if it get the limit
    @task.sensor(poke_interval=30, timeout=600, mode="reschedule")
    def check_api_available() -> PokeReturnValue:
        response = requests.get(f"{base_url}competitions", headers=HEADERS)

        if response.status_code == 200:
            return PokeReturnValue(is_done=True)

        if response.status_code == 429:
            reset_seconds = int(response.headers.get("X-RequestCounter-Reset", 10))

            print(
                f"Rate limit reached. API counter resets in "
                f"{reset_seconds} seconds."
            )

            return PokeReturnValue(is_done=False)

        response.raise_for_status()

    @task(pool="api_pool")
    def get_data(topic: str, season: str):

        url = f"{base_url}{topic}"
        params = {"season": season, "limit": 500}

        start, league, final = topic.split("/")

        s3_key = f"{league}/{season}/{league}_{final}_{season}.json"

        response = requests.get(url, headers=HEADERS, params=params, timeout=30)

        print(f"Request URL: {response.url}")
        print(f"Status code: {response.status_code}")
        print(
            f"Requests remaining: "
            f"{response.headers.get('X-Requests-Available-Minute')}"
        )
        print(f"Counter reset: " f"{response.headers.get('X-RequestCounter-Reset')}")

        if response.status_code == 429:
            reset_seconds = int(response.headers.get("X-RequestCounter-Reset", 10))

            print(f"Going to sleep {reset_seconds + 1} and make request.")

            sleep(reset_seconds + 1)

            response = requests.get(url, headers=HEADERS, params=params, timeout=30)

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

        sleep(7)  # between api requests

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

    @task.bash
    def dbt_build():
        return """
                cd /usr/local/airflow/dbt/football && 
                dbt build --profiles-dir /usr/local/airflow/dbt/profiles --project-dir /usr/local/airflow/dbt/football 
                """

    competitions = ["PL", "SA", "BL1", "FL1", "PD"]

    merged_topics = [
        [
            f"competitions/{competition}/standings",
            f"competitions/{competition}/matches",
            f"competitions/{competition}/scorers",
            f"competitions/{competition}/teams",
        ]
        for competition in competitions
    ]

    topics = [item for sublist in merged_topics for item in sublist]

    # pl_topics = [
    #     "competitions/PL/standings",
    #     "competitions/PL/matches",
    #     "competitions/PL/scorers",
    #     "competitions/PL/teams",
    # ]

    check = check_api_available()

    metadata = get_data.expand(
        topic=topics,
        season=["2023", "2024", "2025", "2026"],
    )

    staging_load = staging_loader.expand(metadata=metadata)

    build_dbt = dbt_build()

    check >> metadata >> staging_load >> build_dbt


elt_football_v1()
