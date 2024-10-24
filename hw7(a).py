from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'SnowflakeELTSessionSummaryDag',
    default_args=default_args,
    description='A DAG to join two tables and create a session summary with deduplication',
    schedule_interval='@daily',  # Adjust as needed
)

# SQL to create the session_summary table in Snowflake (under 'analytics' schema)
create_session_summary_sql = """
CREATE TABLE IF NOT EXISTS analytics.session_summary (
    userId int,
    sessionId varchar(32) primary key,
    channel varchar(32),
    ts timestamp
);
"""

# SQL to join the two tables and insert data into session_summary
# Also checks for duplicates based on sessionId
insert_session_summary_sql = """
INSERT INTO analytics.session_summary (userId, sessionId, channel, ts)
WITH deduplicated_data AS (
    SELECT 
        usc.userId,
        usc.sessionId,
        usc.channel,
        st.ts,
        ROW_NUMBER() OVER (PARTITION BY usc.sessionId ORDER BY st.ts DESC) AS row_num
    FROM stock.stock_data.user_session_channel usc
    JOIN stock.stock_data.session_timestamp st
    ON usc.sessionId = st.sessionId
)
SELECT userId, sessionId, channel, ts 
FROM deduplicated_data
WHERE row_num = 1;
"""

# Define the tasks
start_task = DummyOperator(task_id='start', dag=dag)

# Task to create the session_summary table
create_session_summary_task = SnowflakeOperator(
    task_id='create_session_summary_table',
    snowflake_conn_id='snowflake_conn',  # Update with your Snowflake connection ID
    sql=create_session_summary_sql,
    dag=dag,
)

# Task to join the two tables and load the deduplicated data into session_summary
insert_session_summary_task = SnowflakeOperator(
    task_id='insert_into_session_summary',
    snowflake_conn_id='snowflake_conn',
    sql=insert_session_summary_sql,
    dag=dag,
)

end_task = DummyOperator(task_id='end', dag=dag)

# Task dependencies
start_task >> create_session_summary_task >> insert_session_summary_task >> end_task
