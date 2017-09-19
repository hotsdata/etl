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

tracked_metrics = ['cursedamagedone',
'damagetaken',
'deathcount',
'deaths',
'endofmatchawardmvpboolean',
'escapesperformed',
'experiencecontribution',
'fortsdestroyed',
'gamescore',
'healing',
'herodamage',
'highestkillstreak',
'killcount',
'killcountbuildings',
'killcountminions',
'killcountneutral',
'level',
'match_duration',
'match_lost',
'match_undecided',
'match_won',
'merccampcaptures',
'metaexperience',
'miniondamage',
'onfiretimeonfire',
'outnumbereddeaths',
'playsassassin',
'playsdiablo',
'playsfemale',
'playsmale',
'playsspecialist',
'playsstarcraft',
'playssupport',
'playswarcraft',
'playswarrior',
'protectiongiventoallies',
'regenglobestaken',
'regenmasterstacks',
'secondsdead',
'selfhealing',
'siegedamage',
'solodeathscount',
'solokill',
'structuredamage',
'summondamage',
'takedowns',
'teamfightdamagetaken',
'teamfightescapesperformed',
'teamfighthealingdone',
'teamfightherodamage',
'timeccdenemyheroes',
'timerootingenemyheroes',
'timesilencingenemyheroes',
'timespentdead',
'timestunningenemyheroes',
'totaloutdmg',
'townkills',
'vengeancesperformed',
'watchtowercaptures',
'winsassassin',
'winsdiablo',
'winsfemale',
'winsmale',
'winsspecialist',
'winsstarcraft',
'winssupport',
'winswarcraft',
'winswarrior']

task_variables = {
    'STG_PLAYER_STATS_0': 'stg_player_stats_0',
    'STG_PLAYER_STATS_1': 'stg_player_stats_1',
    'STG_PLAYER_STATS_2': 'stg_player_stats_2',
    'STG_PLAYER_STATS_3': 'stg_player_stats_3',
    'STG_PLAYER_STATS_AGG': 'fct_player_stats_agg',
    'REPLAY_TABLE': 'replayInfo',
    'PLAYERS_TABLE': 'players',
    'LOOKUP_TABLE': 'battletag_toonhandle_lookup',
}

banned_keys = ['playerId',
                'replayId',
                'team',
                'Role',
                'TeamLevel',
                'TimeOnPayload']

dag = DAG(
    'player_stats_agg', default_args=default_args)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='hotstats',
    sql=Template("""

        CREATE TABLE IF NOT EXISTS {{stg_table}} (
            heroName VARCHAR(50),
            replayId VARCHAR(64),
            team SMALLINT,
            metric TEXT,
            value BIGINT,
            process_date TIMESTAMP,
            PRIMARY KEY (process_date, replayId, heroName, team, metric)
        );

        CREATE INDEX IF NOT EXISTS process_date_stg
        ON {{stg_table}} (process_date);

        CREATE TABLE IF NOT EXISTS {{stg_table_1}}
        (
            replayid varchar(64),
            team smallint,
            heroname varchar(50),
            value bigint,
            metric text,
            mapname text,
            gameType text,
            gameLoops integer,
            gameversion integer,
            match_date timestamp WITHOUT time zone,
            process_date date
        )
        ;

        create index IF NOT EXISTS stg_ps_1
            on {{stg_table_1}} (replayid, team, heroname, gameversion)
        ;
        CREATE INDEX IF NOT EXISTS process_date_stg_1
        ON {{stg_table_1}} (process_date);

        CREATE TABLE IF NOT EXISTS {{stg_table_2}}
        (
            heroname varchar(50),
            team smallint,
            replayid varchar(64),
	    value bigint,
	    metric text,
	    process_date date
        )
        ;

        create index IF NOT EXISTS stg_ps_1
            on {{stg_table_2}} (replayid, heroname, team);

        create table IF NOT EXISTS {{stg_table_3}}
        (
            heroname varchar(50),
            mapname text,
            gameversion integer,
            gameType text,
            gameLoops integer,
            value bigint,
            metric text,
            toonhandle text,
            battletag text,
            player_id integer,
            playername text,
            match_date timestamp WITHOUT time zone,
            process_date date
        )
        ;

        CREATE INDEX IF NOT EXISTS process_date_stg_3
        ON {{stg_table_3}} (process_date);

        CREATE TABLE IF NOT EXISTS {{agg_table}} (
            player_id INTEGER,
            toonHandle VARCHAR(64),
            battleTag VARCHAR(64),
            name VARCHAR(50),
            mapname VARCHAR(64),
            gameversion integer,
            match_date TIMESTAMP WITHOUT TIME ZONE,
            gameType TEXT,
            process_date TIMESTAMP WITHOUT TIME ZONE,
            heroName VARCHAR(30),
            metric TEXT,
            value BIGINT,
            games BIGINT,
            PRIMARY KEY (match_date,
                        player_id,
                        toonHandle,
                        name,
                        mapname,
                        gameversion,
                        gameType,
                        heroName,
                        metric)
        );

        CREATE INDEX IF NOT EXISTS match_date_stg_agg
        ON {{agg_table}} (match_date);


    """).render(stg_table=task_variables['STG_PLAYER_STATS_0'],
                stg_table_1=task_variables['STG_PLAYER_STATS_1'],
                stg_table_2=task_variables['STG_PLAYER_STATS_2'],
                stg_table_3=task_variables['STG_PLAYER_STATS_3'],
                agg_table=task_variables['STG_PLAYER_STATS_AGG']),
    dag=dag
)

clean_stg_player_stats_ds = PostgresOperator(
    task_id='clean_stg_player_stats_ds',
    postgres_conn_id='hotstats',
    dag=dag,
    sql=Template("""
                    DELETE FROM {{stg_table}} WHERE process_date = '{{ds}}';
                    DELETE FROM {{stg_table_1}} WHERE process_date = '{{ds}}';
                    DELETE FROM {{stg_table_2}} WHERE process_date = '{{ds}}';
                    DELETE FROM {{stg_table_3}} WHERE process_date = '{{ds}}';
                """).render(stg_table=task_variables['STG_PLAYER_STATS_0'],
                            stg_table_1=task_variables['STG_PLAYER_STATS_1'],
                            stg_table_2=task_variables['STG_PLAYER_STATS_2'],
                            stg_table_3=task_variables['STG_PLAYER_STATS_3'],
                            ds="{{ ds }}")
)

clean_stg_player_stats_ds.set_upstream(create_table)

insert_stg_player_stats_0 = PostgresOperator(
    task_id='insert_stg_player_stats_0',
    postgres_conn_id='hotstats',
    dag=dag,
    sql=Template("""
        INSERT INTO {{stg_table_0}} (heroName, replayId, team, metric, value, process_date)
        SELECT
            heroname,
            replayid,
            team,
            LOWER(key) AS key,
            CASE WHEN value ~ '^[0-9]+$'
                THEN value :: INTEGER
                ELSE 0 END AS value,
            '{{ds}}' AS process_date
        FROM generalstats m, jsonb_each_text(m.doc)
        WHERE value ~ '^[0-9]+$' AND LOWER(key) IN ({{keys}})
    AND CAST(value AS BIGINT) < 2147483647
        AND updated_at at time zone 'utc' BETWEEN '{{ ds }} 00:00:00' AND '{{ ds }} 23:59:59.999'
        --ON CONFLICT ON CONSTRAINT stg_player_stats_pkey DO
          --UPDATE SET metric = excluded.metric;
    """).render(stg_table_0=task_variables['STG_PLAYER_STATS_0'],
                keys=",".join("'%s'" % k for k in tracked_metrics),
                ds="{{ ds }}")
)
insert_stg_player_stats_0.set_upstream(clean_stg_player_stats_ds)

insert_stg_player_stats_1 = PostgresOperator(
    task_id='insert_stg_player_stats_1',
    postgres_conn_id='hotstats',
    dag=dag,
    sql=Template("""
        INSERT INTO {{stg_table_1}}
        (
            replayid,
            team,
            heroname,
            value,
            metric,
            mapname,
            gameType,
            gameLoops,
            gameversion,
            match_date,
            toonHandle,
            playerName,
            battleTag,
            process_date
        )
          SELECT
            A.replayid,
            A.team,
            A.heroname,
            A.value,
            A.metric,
            r.doc ->> 'mapName' AS mapName,
            r.doc ->> 'gameType' AS gameType,
            (r.doc ->> 'gameLoops') :: INTEGER AS gameLoops,
            (r.doc ->> 'gameVersion') :: INTEGER AS gameversion,
            (r.doc ->> 'startTime' :: VARCHAR) :: TIMESTAMP AS match_date,
            p.doc ->> 'toonHandle' :: VARCHAR(64) AS toonHandle,
            p.doc ->> 'name' AS playerName,
            p.doc ->> 'battleTag' :: VARCHAR(64) AS battleTag,
            '{{ds}}' AS process_date
          FROM
            {{stg_table_0}} A
          JOIN {{replay_table}} r
            ON (A.replayid = r.replayid)
          JOIN {{players_table}} p
            ON (p.replayid = A.replayid AND p.team = A.team AND p.heroname = A.heroname)
          WHERE A.process_date = '{{ds}}';

    """).render(stg_table_0=task_variables['STG_PLAYER_STATS_0'],
                stg_table_1=task_variables['STG_PLAYER_STATS_1'],
                replay_table=task_variables['REPLAY_TABLE'],
                players_table=task_variables['PLAYERS_TABLE'],
                ds="{{ ds }}")
)
insert_stg_player_stats_1.set_upstream(insert_stg_player_stats_0)


insert_stg_player_stats_2 = PostgresOperator(
    task_id='insert_player_stats_2',
    postgres_conn_id='hotstats',
    dag=dag,
    sql=Template("""
        INSERT INTO {{stg_table_2}}
        (
            heroname,
	    team,
	    replayid,
            value,
            metric,
            process_date
        )
        SELECT
	   A.heroname,
	   A.team,
	   A.replayid,
	   CASE WHEN (p.doc ->> 'gameResult') :: SMALLINT = 1 THEN 1 ELSE 0 END AS value,
	   'match_won' AS metric,
           DATE('{{ds}}') AS process_date
	FROM {{stg_table_1}} A
	JOIN {{players}} p
	ON (p.replayid = A.replayid AND p.team = A.team AND p.heroname = A.heroname)
	WHERE A.process_date = '{{ds}}'
	GROUP BY 1,2,3,4
	UNION ALL
	SELECT
	   A.heroname,
	   A.team,
	   A.replayid,
	   CASE WHEN (p.doc ->> 'gameResult') :: SMALLINT = 2 THEN 1 ELSE 0 END AS value,
	   'match_lost' AS metric,
           DATE('{{ds}}') AS process_date
	FROM {{stg_table_1}} A
	JOIN {{players}} p
	ON (p.replayid = A.replayid AND p.team = A.team AND p.heroname = A.heroname)
	WHERE A.process_date = '{{ds}}'
	GROUP BY 1,2,3,4
	UNION ALL
	SELECT
	   A.heroname,
	   A.team,
	   A.replayid,
	   CASE WHEN (p.doc ->> 'gameResult') :: SMALLINT NOT IN (1,2) THEN 1 ELSE 0 END AS value,
	   'match_undecided' AS metric,
           DATE('{{ds}}') AS process_ate
	FROM {{stg_table_1}} A
	JOIN {{players}} p
	ON (p.replayid = A.replayid AND p.team = A.team AND p.heroname = A.heroname)
	WHERE A.process_date = '{{ds}}'
	GROUP BY 1,2,3,4
	UNION ALL
	SELECT
	   A.heroname,
	   A.team,
	   A.replayid,
	   ROUND((AVG(A.gameLoops)/16),2) AS value,
	   'match_duration' AS metric,
 	   DATE('{{ds}}') AS process_date
	FROM {{stg_table_1}} A
	JOIN {{players}} p
	ON (p.replayid = A.replayid AND p.team = A.team AND p.heroname = A.heroname)
	WHERE A.process_date = '{{ds}}'
	GROUP BY 1,2,3
    """).render(stg_table_1=task_variables["STG_PLAYER_STATS_1"],
		stg_table_2=task_variables["STG_PLAYER_STATS_2"],
                players=task_variables["PLAYERS_TABLE"],
                ds="{{ ds }}")
)

insert_stg_player_stats_2.set_upstream(insert_stg_player_stats_1)

insert_stg_player_stats_3 = PostgresOperator(
    task_id='insert_stg_player_stats_3',
    postgres_conn_id='hotstats',
    dag=dag,
    sql=Template("""
	INSERT INTO {{stg_table_3}}
	(
	      heroname ,
	      mapname,
	      gameversion,
	      gameType,
	      value,
	      metric,
	      toonhandle,
	      battletag,
	      player_id,
	      playername,
	      match_date,
	      process_date
	  )
	      SELECT
		  A.heroname,
		  A.mapName,
		  A.gameversion,
		  A.gameType,
		  A.value,
		  A.metric,
		  A.toonHandle,
		  A.battleTag,
		  COALESCE(l.player_id,-1) AS player_id,
		  A.playerName,
		  A.match_date,
		  '{{ds}}' AS process_date
	      FROM {{stg_table_1}} A
	      LEFT OUTER JOIN {{lookup}} l
	       ON (A.battletag = l.battletag AND A.toonHandle = l.toonHandle)
	      WHERE A.process_date = '{{ds}}';

	INSERT INTO {{stg_table_3}}
	(
	      heroname ,
	      mapname,
	      gameversion,
	      gameType,
	      value,
	      metric,
	      toonhandle,
	      battletag,
	      player_id,
	      playername,
	      match_date,
	      process_date
	  )
	      SELECT
		  A.heroname,
		  A.mapName,
		  A.gameversion,
		  A.gameType,
		  B.value,
		  B.metric,
		  A.toonHandle,
		  A.battleTag,
		  COALESCE(l.player_id,-1) AS player_id,
		  A.playerName,
		  A.match_date,
		  '{{ds}}' AS process_date
	      FROM {{stg_table_1}} A
	      JOIN {{stg_table_2}} B
	      ON (A.replayid = B.replayid AND A.heroname = B.heroname AND A.team = B.team)
	      LEFT OUTER JOIN {{lookup}} l
	       ON (A.battletag = l.battletag AND A.toonHandle = l.toonHandle)
	      WHERE A.process_date = '{{ds}}'
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11;

    """).render(stg_table_3=task_variables['STG_PLAYER_STATS_3'],
		stg_table_2=task_variables['STG_PLAYER_STATS_2'],
                stg_table_1=task_variables['STG_PLAYER_STATS_1'],
                lookup=task_variables['LOOKUP_TABLE'],
                ds="{{ ds }}")
)
insert_stg_player_stats_3.set_upstream(insert_stg_player_stats_2)

insert_stg_agg = PostgresOperator(
    task_id='insert_stg_agg',
    postgres_conn_id='hotstats',
    dag=dag,
    sql=Template("""

	INSERT INTO {{agg_table}}
	(player_id, toonhandle, battletag, name, mapname, gameversion, match_date, process_date, heroname, stats, games, gametype)
	SELECT
	  player_id,
	  toonhandle,
	  MAX(battletag) AS battletag,
	  playername,
	  mapname,
	  gameversion,
	  match_date,
	  process_date,
	  heroname,
	  jsonb_object_agg(metric, value) AS stats,
	  MAX(games) AS games,
	  gametype
	FROM (
	SELECT
		    A.heroname,
		    A.gameversion,
		    A.gameType,
		    A.toonHandle,
		    A.battleTag,
		    A.player_id,
		    A.mapName,
		    A.playername,
		    A.metric,
		    SUM(A.value) AS value,
		    COUNT(1) AS games,
		    DATE(A.match_date) AS match_date,
		    DATE('{{ds}}') AS process_date
		FROM
		    {{ stg_table_3 }} A
		WHERE process_date = '{{ds}}'
		GROUP BY
		A.heroname,
		A.mapName,
		A.gameversion,
		A.gameType,
		A.toonHandle,
		A.battleTag,
		A.player_id,
		A.playername,
		A.metric,
		DATE('{{ds}}'),
		DATE(A.match_date) ) data
	GROUP BY
	  toonhandle,
	  gameversion,
	  gametype,
	  match_date,
	  mapname,
	  heroname,
	  playername,
	  player_id,
	  process_date
	ON CONFLICT ON CONSTRAINT fct_player_stats_agg_pkey DO
	UPDATE SET
	    stats = excluded.stats
		;
    """).render(stg_table_3=task_variables['STG_PLAYER_STATS_3'],
                agg_table=task_variables['STG_PLAYER_STATS_AGG'],
                ds="{{ ds }}")
)

insert_stg_agg.set_upstream(insert_stg_player_stats_3)

clean_stg_player_stats_ds_final = PostgresOperator(
    task_id='clean_stg_player_stats_ds_final',
    postgres_conn_id='hotstats',
    dag=dag,
    sql=Template("""
                    DELETE FROM {{stg_table}} WHERE process_date = '{{ds}}';
                    DELETE FROM {{stg_table_1}} WHERE process_date = '{{ds}}';
                    DELETE FROM {{stg_table_2}} WHERE process_date = '{{ds}}';
                    DELETE FROM {{stg_table_3}} WHERE process_date = '{{ds}}';
                """).render(stg_table=task_variables['STG_PLAYER_STATS_0'],
                            stg_table_1=task_variables['STG_PLAYER_STATS_1'],
                            stg_table_2=task_variables['STG_PLAYER_STATS_2'],
                            stg_table_3=task_variables['STG_PLAYER_STATS_3'],
                            ds="{{ ds }}")
)


clean_stg_player_stats_ds_final.set_upstream(insert_stg_agg)

end_task = DummyOperator(
    dag=dag,
    task_id='end_task'
)
end_task.set_upstream(clean_stg_player_stats_ds_final)

