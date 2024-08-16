from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator



default_args = {
    'owner' : 'mahak_project1',
    'depends_on_past' : False,
    'start_date' : datetime(2024,7,31),
    'email' : ['mahakajju30@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 2,
    'retry_delay' : timedelta(seconds=15)

}

with DAG(
    dag_id = 'aws_glue_redshift_airflow2',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False
) as dag:

    glue_job_trigger = GlueJobOperator(
        task_id = 'tsk_glue_job_trigger',
        job_name = 'glue_redshift_job',
        aws_conn_id = 'aws_default',
    )
    glue_job_trigger