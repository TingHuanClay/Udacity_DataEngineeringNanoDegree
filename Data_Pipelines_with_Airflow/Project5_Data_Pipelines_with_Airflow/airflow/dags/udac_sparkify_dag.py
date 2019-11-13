from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, CreateTablesOperator)
from helpers import (SqlQueries, CreateTableStaments)

start_date = datetime.utcnow()

"""
    Configuring the DAG: In the DAG, add default parameters according to these guidelines

    1. The DAG does not have dependencies on past runs
    2. On failure, the task are retried 3 times
    3. Retries happen every 5 minutes
    4. Catchup is turned off
    5. Do not email on retry
"""
default_args = {
    'owner': 'udacity_sparkify',
    'start_date': datetime(2018, 1, 1),
    'end_date': datetime(2018, 1, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_sparkify_dag',
          default_args=default_args,
          description='Load and transform data from S3 to Redshift with Airflow',
#           schedule_interval='0 * * * *',
          schedule_interval='@monthly',
          max_active_runs=3
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_event_table = CreateTablesOperator(
    task_id='Create_staging_event_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=CreateTableStaments.CREATE_STAGING_EVENTS_TABLE_SQL
)

create_staging_song_table = CreateTablesOperator(
    task_id='Create_staging_songs_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=CreateTableStaments.CREATE_STAGING_SONGS_TABLE_SQL
)

create_fact_songplays_table = CreateTablesOperator(
    task_id='Create_fact_songplays_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=CreateTableStaments.CREATE_FACT_SONGPLAYS_TABLE_SQL
)

create_dim_artists_table = CreateTablesOperator(
    task_id='Create_dim_artists_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=CreateTableStaments.CREATE_DIM_ARTISTS_TABLE_SQL
)

create_dim_songs_table = CreateTablesOperator(
    task_id='Create_dim_songs_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=CreateTableStaments.CREATE_DIM_SONGS_TABLE_SQL
)


create_dim_time_table = CreateTablesOperator(
    task_id='Create_dim_time_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=CreateTableStaments.CREATE_DIM_TIME_TABLE_SQL
)


create_dim_users_table = CreateTablesOperator(
    task_id='Create_dim_users_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=CreateTableStaments.CREATE_DIM_USERS_TABLE_SQL
)

dummy_operator = DummyOperator(task_id='Dummy_After_CreateTable',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,

    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    select_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    truncate_table=True,
    select_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    truncate_table=True,
    select_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    truncate_table=True,
    select_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    truncate_table=True,
    select_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=["songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Define task ordering for the DAG tasks you defined
#

start_operator >> [create_staging_event_table, create_staging_song_table,
                   create_fact_songplays_table,
                   create_dim_artists_table, create_dim_songs_table,
                   create_dim_time_table, create_dim_users_table] >> dummy_operator

dummy_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator

