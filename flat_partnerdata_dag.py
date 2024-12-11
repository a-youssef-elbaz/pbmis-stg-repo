from airflow import DAG
from airflow.models.param import Param
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator, DataprocSubmitJobOperator
from datetime import timedelta

# Default DAG arguments
DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
with DAG(
    dag_id="Flat_Partnerdata",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    params={
        "project_id": Param(default="", type="string"),
        "cluster_name": Param(default="", type="string"),
        "region": Param(default="europe-west3", type="string"),
        "book_date": Param(default="2020-01-01", type="string"),
        "load_user": Param(default="", type="string"),
    },
    render_template_as_native_obj=True,
) as dag:

    # Submit the PySpark job
    spark_job = DataprocSubmitJobOperator(
        task_id="Flat_Partnerdata_Spark_Job",
        project_id="{{ params.project_id }}",  # Dynamic, evaluated at runtime
        region="{{ params.region }}",  # Dynamic, evaluated at runtime
        job={
            "reference": {"project_id": "{{ params.project_id }}"},  # Dynamic, evaluated at runtime
            "placement": {"cluster_name": "{{ params.cluster_name }}"},  # Dynamic, evaluated at runtime
            "pyspark_job": {
                "jar_file_uris": [
                    # Required for BigQuery connect with Spark
                    "gs://db-dev-europe-west3-gcs-144024-pbmis-dataproc-codebase-ahmed/jars/spark-bigquery-with-dependencies_2.12-0.34.0.jar"
                ],
                "main_python_file_uri": "gs://db-dev-europe-west3-gcs-144024-pbmis-dataproc-codebase-ahmed/pbmis-utils/src/partnerdata_flat_process.py",
                "args": [
                    # Arguments passed to  flat partnerdata PySpark script
                    "--project_id", "{{ params.project_id }}",
                    "--book_date", "{{ params.book_date }}",
                    "--load_user", "{{ params.load_user }}"
                ],
            }
        }
    )

