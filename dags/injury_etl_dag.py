from airflow.sdk import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import sys
import os

# Add the parent directory of this file to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from injury_etl.extract import extract_injury_data, extract_match_data, extract_understat_player_stats
from injury_etl.transform import transform_match_data, transform_player_stats, transform_injury_data
from injury_etl.load import insert_matches, insert_player_details, insert_player_stats, load_injuries_data, create_injury_schema, load_teams
from injury_etl.utils import get_db_connection, get_team_name_to_id

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=0.25),
}

with DAG(
    dag_id="injury_etl_dag",
    description="ETL pipeline for Understat and Premier Injuries data",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["injury", "understat", "ETL"]
) as dag:

    @task()
    def extract_task(**kwargs):
        injury_html = extract_injury_data()
        matches_json = extract_match_data()
        players_raw = extract_understat_player_stats()

        kwargs["ti"].xcom_push(key="injury_html", value=injury_html.prettify())
        kwargs["ti"].xcom_push(key="matches_json", value=matches_json)
        kwargs["ti"].xcom_push(key="players_raw", value=players_raw)

    @task()
    def transform_task(**kwargs):
        from bs4 import BeautifulSoup

        ti = kwargs["ti"]
        conn = get_db_connection()
        team_name_to_id = get_team_name_to_id(conn)

        # Load XComs
        injury_html = ti.xcom_pull(key="injury_html", task_ids="extract_task")
        matches_json = ti.xcom_pull(key="matches_json", task_ids="extract_task")
        players_raw = ti.xcom_pull(key="players_raw", task_ids="extract_task")

        # Transform
        match_data = transform_match_data(matches_json, team_name_to_id)
        players, stats = transform_player_stats(players_raw, team_name_to_id, conn)
        injuries = transform_injury_data(BeautifulSoup(injury_html, "lxml"), conn, team_name_to_id)

        # Push to XComs
        ti.xcom_push(key="matches", value=match_data)
        ti.xcom_push(key="players", value=players)
        ti.xcom_push(key="player_stats", value=stats)
        ti.xcom_push(key="injuries", value=injuries)

    @task()
    def load_task(**kwargs):
        ti = kwargs["ti"]
        conn = get_db_connection()

        matches = ti.xcom_pull(key="matches", task_ids="transform_task")
        players = ti.xcom_pull(key="players", task_ids="transform_task")
        player_stats = ti.xcom_pull(key="player_stats", task_ids="transform_task")
        injuries = ti.xcom_pull(key="injuries", task_ids="transform_task")

        with conn.cursor() as cur:
            create_injury_schema(conn)
            load_teams(cur)
            insert_matches(cur, matches)
            insert_player_details(cur, players)
            insert_player_stats(cur, player_stats)
            conn.commit()

        load_injuries_data(conn, injuries)

    # Set dependencies
    extract_task() >> transform_task() >> load_task()
