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
    'METRICS_TABLE': 'stats_historical_metrics',
    'STG_PLAYER_STATS_AGG': 'stg_player_stats_agg',
}

dag = DAG(
    'global_stats_calculation', default_args=default_args)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='hotstats',
    sql=Template("""

        CREATE TABLE IF NOT EXISTS {{metrics_table}}
        (
            heroname varchar(30) not null,
            mapname varchar(64) not null,
            metric text not null,
            mpg_l30 numeric,
            mpg_l60 numeric,
            mpg_l90 numeric,
            mpg_l180 numeric,
            games_l30 bigint,
            games_l60 bigint,
            games_l90 bigint,
            games_l180 bigint,
            metric_l30 numeric,
            metric_l60 numeric,
            metric_l90 numeric,
            metric_l180 numeric,
            constraint stats_historical_metrics_heroname_mapname_metric_pk
                primary key (heroname, mapname, metric)
        )
        ;
    """).render(metrics_table=task_variables['METRICS_TABLE']),
    dag=dag
)


insert_global_stats = PostgresOperator(
    task_id='insert_global_stats',
    postgres_conn_id='hotstats',
    dag=dag,
    sql=Template("""
        INSERT INTO {{metrics_table}}
        (
            heroname,
            mapname,
            metric,
            mpg_l30,
            mpg_l60,
            mpg_l90,
            mpg_l180,
            games_l30,
            games_l60,
            games_l90,
            games_l180,
            metric_l30,
            metric_l60,
            metric_l90,
            metric_l180
        )
        SELECT
          heroname,
          mapname,
          metric,
          ROUND(1.0 * (metric_l30/CASE games_l30 WHEN 0 THEN 1 ELSE games_l30 END),2) AS mpg_l30,
          ROUND(1.0 * (metric_l60/CASE games_l60 WHEN 0 THEN 1 ELSE games_l60 END),2) AS mpg_l60,
          ROUND(1.0 * (metric_l90/CASE games_l90 WHEN 0 THEN 1 ELSE games_l90 END),2) AS mpg_l90,
          ROUND(1.0 * (metric_l180/CASE games_l180 WHEN 0 THEN 1 ELSE games_l180 END),2) AS mpg_l180,
          games_l30, games_l60, games_l90, games_l180,
          metric_l30, metric_l60, metric_l90, metric_l180
        FROM (
          SELECT
            heroname,
            mapname,
            metric,
            SUM(CASE WHEN
              DATE((match_date AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
                BETWEEN ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC') - interval '30 day'
                  AND
                ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
              THEN value ELSE 0
            END) AS metric_l30,
            SUM(CASE WHEN
              DATE((match_date AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
                BETWEEN ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC') - interval '60 day'
                  AND
                ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
              THEN value ELSE 0
            END) AS metric_l60,
            SUM(CASE WHEN
              DATE((match_date AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
                BETWEEN ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC') - interval '90 day'
                  AND
                ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
              THEN value ELSE 0
            END) AS metric_l90
          ,
            SUM(CASE WHEN
              DATE((match_date AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
                BETWEEN ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC') - interval '180 day'
                  AND
                ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC-07:00') AT TIME ZONE 'UTC')
              THEN value ELSE 0
            END) AS metric_l180,
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
          FROM {{agg_table}}
          GROUP BY heroname, mapname, metric
          ) metrics
          ON CONFLICT ON CONSTRAINT stats_historical_metrics_heroname_mapname_metric_pk DO
          UPDATE SET mpg_l30 = excluded.mpg_l30,
          mpg_l60 = excluded.mpg_l60,
          mpg_l90 = excluded.mpg_l90,
          mpg_l180 = excluded.mpg_l180,
          games_l30 = excluded.games_l30,
          games_l60 = excluded.games_l60,
          games_l90 = excluded.games_l90,
          games_l180 = excluded.games_l180,
          metric_l30 = excluded.metric_l30,
          metric_l60 = excluded.metric_l60,
          metric_l90 = excluded.metric_l90,
          metric_l180 = excluded.metric_l180;
    """).render(metrics_table=task_variables['METRICS_TABLE'],
                agg_table=task_variables['STG_PLAYER_STATS_AGG'],
                ds="{{ ds }}")
)
insert_global_stats.set_upstream(create_table)

end_task = DummyOperator(
    dag=dag,
    task_id='end_task'
)
end_task.set_upstream(insert_global_stats)

