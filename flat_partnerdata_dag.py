import logging
import os
from airflow import DAG
from airflow.models.param import Param
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import timedelta

# Default arguments for the DAG
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
    dag_id="generic_flattening_dag_v2",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    params={
        "project_id": Param(default="your_project", type="string"),
        "book_date": Param(default="2024-01-01", type="string"),
        "destination_table": Param(default="your_dataset.flattened_table", type="string"),
    },
    render_template_as_native_obj=True,
) as dag:
    from google.cloud import storage


    def load_query(file_nm, file_path, bucket_name, params):
        """
        Load a SQL query file from GCS.
        :param file_nm: The name of the SQL file (without extension).
        :param file_path: The directory path in GCS where the file is stored.
        :param bucket_name: The name of the GCS bucket.
        :return: The content of the SQL file as a string.
        """

        full_path = f"{file_path}/{file_nm}"
        logging.info(f"loading the flatenning query file {file_nm} from this path {full_path}")

        # Create a GCS client and access the bucket
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(full_path)

        # Check if the file exists in GCS
        if not blob.exists():
            raise FileNotFoundError(f"File '{full_path}' not found in bucket '{bucket_name}'")

        sql_query = blob.download_as_text()

        # Replace placeholders with actual parameter values
        for key, value in params.items():
            placeholder = f"{{{{ {key} }}}}"  # Double curly braces for Jinja-style placeholders
            sql_query = sql_query.replace(placeholder, value)

        # Download and return the file content as a string
        return sql_query


    # load the SQL query based on source and path
    bucket_name = "europe-west3-rdl-composerv2-246f9af4-bucket"
    file_path = "config/framework/extraction/partner"
    file_name = "flat_partnerdata.sql"
    FLATTENING_QUERY = load_query(file_name, file_path, bucket_name)

    # BigQuery Task
    flattening_job = BigQueryInsertJobOperator(
        task_id="partnerdata_flattening_query",
        configuration={
            "query": {
                "query": FLATTENING_QUERY,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "{{ params.project_id }}",
                    "datasetId": "ds_pbmis_daily",
                    "tableId": "t_pbmis_partnerdata",
                },
                "timePartitioning": {
                    "type": "DAY",
                    "field": "book_date",
                },
                "writeDisposition": "WRITE_APPEND",
            }
        },
        location="europe-west3",  # flat Bigquery region
    )