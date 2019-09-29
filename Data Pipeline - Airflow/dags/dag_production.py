from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries
from airflow.operators import (StageToRedshiftOperator, 
                               LoadFactOperator,
                               LoadDimensionOperator, 
                               DataQualityOperator,
                               CreateTableRedshiftOperator)

# : Things you will need to add to the Airflow environment-
# : 1. aws_credentials    --> You will need to store it as Amazon Web Service by having 
# :                           the user and password as the Key and secret Key
# :
# : 2. redshift           --> Store this as Postgres connection and enter in the host, schema name,  
# :                           login to the redshift and the port 5439


# all_table_config is the configuration to create the tables. Check it out below!
all_table_config = '/home/workspace/airflow/data_config/sql_config.txt'

default_args = {
    'owner': 'Dan Pesiwarissa',
    'start_date': datetime.now(),
    'depends_on_past' : True,
    'retries' : 1,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False
}

dag = DAG('Sparkify_Data_Pipeline',
          default_args=default_args,
          description='Main production line for the Data Pipeline',
          schedule_interval='@hourly')

# This is a dummy operator, it's an empty node
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# This node creates all of the necessary tables
create_table_task = CreateTableRedshiftOperator(task_id='create_tables_task',
                                                dag=dag,
                                                redshift_conn_id="redshift",
                                                table_config=all_table_config)

# There are 2 staging tables and we're creating them below
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_creds='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_format='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_creds='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data'
)

# Below are the loading nodes to allow all of the into each tables
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    table=SqlQueries.songplay_table_insert,
    aws_creds='aws_credentials',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    table=SqlQueries.user_table_insert,
    aws_creds='aws_credentials',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    table=SqlQueries.song_table_insert,
    aws_creds='aws_credentials',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    table=SqlQueries.artist_table_insert,
    aws_creds='aws_credentials',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    table=SqlQueries.time_table_insert,
    aws_creds='aws_credentials',
    dag=dag
)

# Below are the QA checks for all of the tables
run_quality_checks_artists = DataQualityOperator(
    task_id='Run_data_quality_checks_artists',
    dag=dag,
    redshift_conn_id='redshift',
    aws_creds='aws_credentials',
    table='artists'
)

run_quality_checks_users = DataQualityOperator(
    task_id='Run_data_quality_checks_users',
    dag=dag,
    redshift_conn_id='redshift',
    aws_creds='aws_credentials',
    table='users'
)

run_quality_checks_songs = DataQualityOperator(
    task_id='Run_data_quality_checks_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_creds='aws_credentials',
    table='songs'
)

run_quality_checks_time = DataQualityOperator(
    task_id='Run_data_quality_checks_time',
    dag=dag,
    redshift_conn_id='redshift',
    aws_creds='aws_credentials',
    table='time'
)

run_quality_checks_songplays = DataQualityOperator(
    task_id='Run_data_quality_checks_songplays',
    dag=dag,
    redshift_conn_id='redshift',
    aws_creds='aws_credentials',
    table='songplays'
)

# End node
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Starts from the dummy operator to create tables
start_operator >> create_table_task

# Once creating the tables completed, we then load the raw data to staging table
create_table_task >> stage_events_to_redshift
create_table_task >> stage_songs_to_redshift

# Below are the steps of the nodes and edges
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> run_quality_checks_songplays

run_quality_checks_songplays >> load_user_dimension_table
run_quality_checks_songplays >> load_song_dimension_table
run_quality_checks_songplays >> load_artist_dimension_table
run_quality_checks_songplays >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks_users
load_song_dimension_table >> run_quality_checks_songs
load_artist_dimension_table >> run_quality_checks_artists
load_time_dimension_table >> run_quality_checks_time

run_quality_checks_users >> end_operator
run_quality_checks_songs >> end_operator
run_quality_checks_artists >> end_operator
run_quality_checks_time >> end_operator