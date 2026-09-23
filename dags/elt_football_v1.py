import os
from datetime import datetime, timedelta, timezone
from time import sleep
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.sensors.base import PokeReturnValue
import pendulum
import requests
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import yaml
import pendulum

base_url = "http://api.football-data.org/v4/"

BUCKET_NAME = "bronze"


# for the first run only it would be schedule yearly, with start date of 2023
@dag(
    start_date=pendulum.datetime(2026, 1, 1, tz="Africa/Cairo"),
    schedule="0 6 * * *",
    catchup=False,
    default_args={
        "owner": "football_team",
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=2),
    },
)
def elt_football_v1():

    # extract data from api for the topic needed and put it into bronze bucket
    @task(pool="api_pool")
    def get_data(topic: str, logical_date=None):
        API_TOKEN = os.getenv("X-Auth-Token")

        if not API_TOKEN:
            raise AirflowFailException(f"API Token enviromnet variable is missing")

        headers = {"X-Auth-Token": API_TOKEN}

        url = f"{base_url}{topic}"

        season = logical_date.year

        params = {"season": season, "limit": 500}

        start, league, final = topic.split("/")

        s3_key = f"{league}/{season}/{league}_{final}_{season}.json"

        response = requests.get(url, headers=headers, params=params, timeout=30)

        print(f"Request URL: {response.url}")
        print(f"Status code: {response.status_code}")
        print(
            f"Requests remaining: "
            f"{response.headers.get('X-Requests-Available-Minute')}"
        )
        print(f"Counter reset: " f"{response.headers.get('X-RequestCounter-Reset')}")

        # Success
        if response.status_code == 200:
            # check for the json file
            try:
                data = (
                    response.json()
                )  # this will raise "requests.exceptions.JSONDecodeError" if there is issue
            except json.JSONDecodeError as exc:
                raise AirflowFailException(
                    f"Invalid returned JSON "
                    f"url={response.url} "
                    f"response={response.text[:500]}"
                ) from exc

        # config or auth problems
        elif response.status_code in (400, 401, 403, 404):
            raise AirflowFailException(
                f"API request failed with HTTP "
                f"{response.status_code} "
                f"{response.text[:500]}"
            )

        # limit reached
        elif response.status_code == 429:
            reset_seconds = response.headers.get("X-RequestCounter-Reset", "unknown")
            raise AirflowException(
                f"API limit reached " f"will reset in {reset_seconds} sec"
            )

        # server side errors
        elif 500 <= response.status_code <= 599:
            raise AirflowException(
                f"API server error " f"{response.status_code} " f"{response.text[:500]}"
            )

        else:
            raise AirflowFailException(
                f"unexpected API response "
                f"{response.status_code} "
                f"{response.text[:500]}"
            )

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

    @task.bash
    def dbt_build():
        return """
                cd /usr/local/airflow/dbt/football && 
                dbt build --profiles-dir /usr/local/airflow/dbt/profiles --project-dir /usr/local/airflow/dbt/football 
                """

    ################################
    ################################

    CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config", "config.yaml")

    with open(CONFIG_PATH, "r") as conf_file:
        config = yaml.safe_load(conf_file)

    competitions = [competition for competition in config["competitions"]]
    seasons = [season for season in config["seasons"]]

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

    metadata = get_data.expand(topic=topics)

    staging_load = staging_loader.expand(metadata=metadata)

    build_dbt = dbt_build()

    metadata >> staging_load >> build_dbt


elt_football_v1()
