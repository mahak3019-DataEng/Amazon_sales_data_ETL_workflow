from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator


def glue_job_s3_redshift_transfer(job_name):
    session = AWSGenericHook(aws_conn_id = '')


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
    dag_id = 'aws_glue_redshift_airflow',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False
) as dag:

    glue_job_trigger = PythonOperator(
        task_id = 'tsk_glue_job_trigger',
        python_callable = glue_job_s3_redshift_transfer,
        op_kwargs = {
            'job_name' : 'glue_redshift_job'
        }
    )