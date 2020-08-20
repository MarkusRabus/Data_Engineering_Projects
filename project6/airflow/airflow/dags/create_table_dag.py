from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator)
#import sql_statements
from helpers import SqlQueriesTables

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 0,
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('create_table_dag',
          default_args=default_args,
          description='Create tables in Redshift with Airflow',
          schedule_interval=None,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

dropTableStagingEvents = PostgresOperator(
    task_id="dropTableStagingEvents",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueriesTables.STAGING_EVENTS_DROP_SQL
)
createTableStagingEvents = PostgresOperator(
    task_id="createTableStagingEvents",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueriesTables.CREATE_STAGING_EVENTS_TABLE_SQL
)

dropTableStagingSongs = PostgresOperator(
    task_id="dropTableStagingSongs",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueriesTables.STAGING_SONGS_DROP_SQL
)
createTableStagingSongs = PostgresOperator(
    task_id="createTableStagingSongs",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueriesTables.CREATE_STAGING_SONGS_TABLE_SQL
)

dropTableSongplays = PostgresOperator(
    task_id="dropTableSongplays",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueriesTables.SONGPLAYS_DROP_SQL
)
createTableSongplays = PostgresOperator(
    task_id="createTableSongplays",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueriesTables.CREATE_SONGPLAYS_TABLE_SQL
)


dropTableUsers = PostgresOperator(
    task_id="dropTableUsers",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueriesTables.USERS_DROP_SQL
)
createTableUsers = PostgresOperator(
    task_id="createTableUsers",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueriesTables.CREATE_USERS_TABLE_SQL
)


dropTableSongs = PostgresOperator(
    task_id="dropTableSongs",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueriesTables.SONGS_DROP_SQL
)
createTableSongs = PostgresOperator(
    task_id="createTableSongs",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueriesTables.CREATE_SONGS_TABLE_SQL
)


dropTableArtists = PostgresOperator(
    task_id="dropTableArtists",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueriesTables.ARTISTS_DROP_SQL
)
createTableArtists = PostgresOperator(
    task_id="createTableArtists",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueriesTables.CREATE_ARTISTS_TABLE_SQL
)


dropTableTime = PostgresOperator(
    task_id="dropTableTime",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueriesTables.TIME_DROP_SQL
)
createTableTime = PostgresOperator(
    task_id="createTableTime",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueriesTables.CREATE_TIME_TABLE_SQL
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> dropTableStagingEvents
start_operator >> dropTableStagingSongs
start_operator >> dropTableSongplays
start_operator >> dropTableUsers
start_operator >> dropTableSongs
start_operator >> dropTableArtists
start_operator >> dropTableTime

createTableStagingEvents << dropTableStagingEvents
createTableStagingSongs << dropTableStagingSongs
createTableSongplays << dropTableSongplays
createTableUsers << dropTableUsers
createTableSongs << dropTableSongs
createTableArtists << dropTableArtists
createTableTime << dropTableTime

createTableStagingEvents >> end_operator
createTableStagingSongs >> end_operator
createTableSongplays >> end_operator
createTableUsers >> end_operator


createTableSongs >> end_operator
createTableArtists >> end_operator
createTableTime >> end_operator


