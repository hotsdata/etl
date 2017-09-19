"""
This pipeline populates stats_hero_bans table, this is a daily process that grabs all the uploads
that happened since the last run and aggregates on a daily, player, hero, map, patch and gametype level.
The metrics that are aggregated are coming from the replayinfo and teamgeneralstats table.


"""
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from jinja2 import Template


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 7, 4),
    'email': ['crorella@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=60),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    'priority_weight': 10,
}

task_variables = {
    'METRICS_TABLE': 'stats_hero_bans',
    'REPLAYINFO': 'replayinfo',
    'TEAMSTATS': 'teamgeneralstats'
}

dag = DAG(
    'hero_bans_calculations', default_args=default_args)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='hotstats',
    sql=Template("""
        CREATE TABLE IF NOT EXISTS {{metrics_table}}
        (
            heroname VARCHAR(64),
            mapname VARCHAR(64),
            game_type VARCHAR(25),
            gameversion INTEGER,
            banned_games INTEGER,
            constraint stats_hero_bans_pk
                primary key (heroname, mapname, game_type, gameversion)
        )
        ;

    """).render(metrics_table=task_variables['METRICS_TABLE']),
    dag=dag
)


insert_banned_heroes = PostgresOperator(
    task_id='insert_banned_heroes',
    postgres_conn_id='hotstats',
    dag=dag,
    sql=Template("""
        INSERT INTO {{metrics_table}}
        (
            heroname,
            mapname,
            game_type,
            gameversion,
            banned_games
        )

        SELECT heroes.name AS banned_hero, map_name, game_type, game_version, COUNT(*) AS banned_games
        FROM
          (
            SELECT
              jsonb_array_elements(tgs.doc -> 'banned') AS banned_hero,
              r.doc->>'gameType'::varchar AS game_type,
              (r.doc->>'gameVersion') :: INTEGER AS game_version,
              r.doc->>'mapName' AS map_name
            FROM {{team_table}} tgs
            JOIN {{info_table}} r ON (tgs.replayid = r.replayid)
                                   AND tgs.doc ->> 'banned' IS NOT NULL
            ORDER BY r.updated_at) source_data
            JOIN heroes ON (LOWER(REGEXP_REPLACE(source_data.banned_hero :: VARCHAR, '"', '', 'g')) = heroes.attrib_name)
        GROUP BY heroes.name, game_type, game_version, map_name
        ON CONFLICT ON CONSTRAINT stats_hero_bans_pk DO
        UPDATE SET
        banned_games = excluded.banned_games
    """).render(metrics_table=task_variables['METRICS_TABLE'],
                team_table=task_variables['TEAMSTATS'],
                info_table=task_variables['REPLAYINFO'])
)
insert_banned_heroes.set_upstream(create_table)

end_task = DummyOperator(
    dag=dag,
    task_id='end_task'
)
end_task.set_upstream(insert_banned_heroes)
