from datetime import timedelta  
import airflow  
from airflow import DAG  
from airflow.providers.amazon.aws.sensors.s3_prefix import S3PrefixSensor
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator

# Custom Operators deployed as Airflow plugins
from awsairflowlib.operators.aws_copy_s3_to_redshift import CopyS3ToRedshiftOperator


S3_BUCKET_NAME = "mwaa-demo-buckets"  
  
default_args = {  
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(  
    'demo_simple_mwaa_2',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 3 * * *'
)

s3_sensor = S3PrefixSensor(  
  task_id='s3_sensor',  
  bucket_name=S3_BUCKET_NAME,  
  prefix='data/raw/green',  
  dag=dag  
)

config = {"Name": "airflow-workshop-raw-green-crawler"}

glue_crawler = AwsGlueCrawlerOperator(
    task_id="glue_crawler",
    config=config,
    dag=dag)

glue_task = AwsGlueJobOperator(  
    task_id="glue_task",  
    job_name='nyc_raw_to_transform',  
    iam_role_name='AWSGlueServiceRoleDefault',  
    dag=dag) 

copy_s3_to_redshift = CopyS3ToRedshiftOperator(
    task_id='copy_to_redshift',
    redshift_conn_id='redshift_serverless',
    schema='nyc',
    table='green',
    s3_bucket=S3_BUCKET_NAME,
    s3_key='data/transformed',
    iam_role_arn='arn\:aws\:iam::024979932511\:role/AmazonMWAA-workshop-redshift-role',
    copy_options=["FORMAT AS PARQUET"],
    dag=dag,
)

s3_sensor >> glue_crawler >> glue_task >> copy_s3_to_redshift