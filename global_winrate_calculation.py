"""
This pipeline populates fct_players_stats table, this is a daily process that grabs all the uploads
that happened since the last run and aggregates on a daily, player, hero, map and gametype level.
The metrics that are aggregated are coming from the generalstats table.


"""
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from jinja2 import Template


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
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
    'METRICS_TABLE': 'stats_historical_winrates',
    'STG_PLAYER_STATS_AGG': 'stg_player_stats_agg',
}

dag = DAG(
    'global_winrate_calculation', default_args=default_args)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='hotstats',
    sql=Template("""
        CREATE TABLE IF NOT EXISTS stats_historical_winrates
        (
            toonhandle varchar(64) not null,
            heroname varchar(64) not null,
            mapname varchar(64) not null,
            game_type varchar(25) not null,
            gameversion integer,
            winrate_l30 numeric,
            winrate_l60 numeric,
            winrate_l90 numeric,
            winrate_l180 numeric,
            games_l30 bigint,
            games_l60 bigint,
            games_l90 bigint,
            games_l180 bigint,
            wins_l30 numeric,
            wins_l60 numeric,
            wins_l90 numeric,
            wins_l180 numeric,
            constraint stats_historical_winrates_toonhandle_mapname_pk
                primary key (toonhandle, heroname, mapname, game_type, gameversion)
        )
        ;

    """).render(metrics_table=task_variables['METRICS_TABLE']),
    dag=dag
)


insert_global_winrates = PostgresOperator(
    task_id='insert_global_winrates',
    postgres_conn_id='hotstats',
    dag=dag,
    sql=Template("""
        INSERT INTO {{metrics_table}}
        (
            toonhandle,
	    mapname,
            heroname,
            game_type,
            gameversion,
            winrate_l30,
            winrate_l60,
            winrate_l90,
            winrate_l180,
            games_l30,
            games_l60,
            games_l90,
            games_l180,
            wins_l30,
            wins_l60,
            wins_l90,
            wins_l180
        )
        SELECT
          toonhandle,
          mapname,
	  heroname,
          gameType,
          gameversion,
          ROUND(1.0 * (wins_l30/CASE games_l30 WHEN 0 THEN 1 ELSE games_l30 END),2) AS winrate_l30,
          ROUND(1.0 * (wins_l60/CASE games_l60 WHEN 0 THEN 1 ELSE games_l60 END),2) AS winrate_l60,
          ROUND(1.0 * (wins_l90/CASE games_l90 WHEN 0 THEN 1 ELSE games_l90 END),2) AS winrate_l90,
          ROUND(1.0 * (wins_l180/CASE games_l180 WHEN 0 THEN 1 ELSE games_l180 END),2) AS winrate_l180,
          games_l30, games_l60, games_l90, games_l180,
          wins_l30, wins_l60, wins_l90, wins_l180
        FROM (
        SELECT
          toonhandle,
          mapname,
          heroname,
          gameType,
          gameversion,
          SUM(CASE WHEN
            DATE((match_date AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
              BETWEEN ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC') - interval '30 day'
                AND
              ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
            THEN value ELSE 0
          END) AS wins_l30,
          SUM(CASE WHEN
            DATE((match_date AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
              BETWEEN ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC') - interval '60 day'
                AND
              ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
            THEN value ELSE 0
          END) AS wins_l60,
          SUM(CASE WHEN
            DATE((match_date AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
              BETWEEN ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC') - interval '90 day'
                AND
              ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
            THEN value ELSE 0
          END) AS wins_l90,
          SUM(CASE WHEN
            DATE((match_date AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
              BETWEEN ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC') - interval '180 day'
                AND
              ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
            THEN value ELSE 0
          END) AS wins_l180,
          SUM(CASE WHEN
            DATE((match_date AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
              BETWEEN ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC') - interval '30 day'
                AND
              ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
            THEN 1 ELSE 0
          END) AS games_l30,
          SUM(CASE WHEN
            DATE((match_date AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
              BETWEEN ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC') - interval '60 day'
                AND
              ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
            THEN 1 ELSE 0
          END) AS games_l60,
          SUM(CASE WHEN
            DATE((match_date AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
              BETWEEN ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC') - interval '90 day'
                AND
              ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
            THEN 1 ELSE 0
          END) AS games_l90,
          SUM(CASE WHEN
            DATE((match_date AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
              BETWEEN ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC') - interval '180 day'
                AND
              ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
            THEN 1 ELSE 0
          END) AS games_l180
        FROM {{agg_table}} WHERE metric = 'match_won'
        GROUP BY heroname, mapname, toonhandle, gameType, gameversion ) winrates
        ON CONFLICT ON CONSTRAINT stats_historical_winrates_toonhandle_mapname_pk DO
        UPDATE SET
        winrate_l30 = excluded.winrate_l30,
        winrate_l60 = excluded.winrate_l60,
        winrate_l90 = excluded.winrate_l90,
        winrate_l180 = excluded.winrate_l180,
        games_l30 = excluded.games_l30,
        games_l60 = excluded.games_l60,
        games_l90 = excluded.games_l90,
        games_l180 = excluded.games_l180,
        wins_l30 = excluded.wins_l30,
        wins_l60 = excluded.wins_l60,
        wins_l90 = excluded.wins_l90,
        wins_l180 = excluded.wins_l180;
    """).render(metrics_table=task_variables['METRICS_TABLE'],
                agg_table=task_variables['STG_PLAYER_STATS_AGG'],
                ds="{{ ds }}")
)
insert_global_winrates.set_upstream(create_table)

end_task = DummyOperator(
    dag=dag,
    task_id='end_task'
)
end_task.set_upstream(insert_global_winrates)
