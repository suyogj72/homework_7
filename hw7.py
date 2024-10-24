from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'SnowflakeETLDag',
    default_args=default_args,
    description='A DAG to load data into Snowflake from S3',
    schedule_interval='@daily',  # Adjust as needed
)

# SQL to create the user_session_channel table in Snowflake
create_user_session_channel_sql = """
CREATE TABLE IF NOT EXISTS stock.stock_data.user_session_channel ( 
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'  
);
"""

# SQL to create the session_timestamp table in Snowflake
create_session_timestamp_sql = """
CREATE TABLE IF NOT EXISTS stock.stock_data.session_timestamp ( 
    sessionId varchar(32) primary key,
    ts timestamp  
);
"""

# SQL to create or replace Snowflake stage pointing to S3
create_stage_sql = """
CREATE OR REPLACE STAGE stock.stock_data.blob_stage 
url = 's3://s3-geospatial/readonly/'
file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
"""

# SQL to copy data into the user_session_channel table from the S3 bucket
copy_to_user_session_channel_sql = """
COPY INTO stock.stock_data.user_session_channel 
FROM @stock.stock_data.blob_stage/user_session_channel.csv;
"""

# SQL to copy data into the session_timestamp table from the S3 bucket
copy_to_session_timestamp_sql = """
COPY INTO stock.stock_data.session_timestamp 
FROM @stock.stock_data.blob_stage/session_timestamp.csv;
"""

# Define the tasks
start_task = DummyOperator(task_id='start', dag=dag)

create_user_session_channel_task = SnowflakeOperator(
    task_id='create_user_session_channel_table',
    snowflake_conn_id='snowflake_conn',  # Update with your connection ID
    sql=create_user_session_channel_sql,
    dag=dag,
)

create_session_timestamp_task = SnowflakeOperator(
    task_id='create_session_timestamp_table',
    snowflake_conn_id='snowflake_conn',
    sql=create_session_timestamp_sql,
    dag=dag,
)

create_stage_task = SnowflakeOperator(
    task_id='create_s3_stage',
    snowflake_conn_id='snowflake_conn',
    sql=create_stage_sql,
    dag=dag,
)

copy_data_to_user_session_channel_task = SnowflakeOperator(
    task_id='copy_data_to_user_session_channel',
    snowflake_conn_id='snowflake_conn',
    sql=copy_to_user_session_channel_sql,
    dag=dag,
)

copy_data_to_session_timestamp_task = SnowflakeOperator(
    task_id='copy_data_to_session_timestamp',
    snowflake_conn_id='snowflake_conn',
    sql=copy_to_session_timestamp_sql,
    dag=dag,
)

end_task = DummyOperator(task_id='end', dag=dag)

# Define task dependencies
start_task >> create_user_session_channel_task >> create_session_timestamp_task >> create_stage_task
create_stage_task >> copy_data_to_user_session_channel_task >> copy_data_to_session_timestamp_task
copy_data_to_session_timestamp_task >> end_task
