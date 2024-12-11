def read_file_from_gcs(bucket_name, file_path):
    # Initialize a GCS client
    client = storage.Client()

    # Get the bucket
    bucket = client.get_bucket(bucket_name)

    # Get the blob (file) from the bucket
    blob = bucket.blob(file_path)

    # Read the content of the file as a string
    file_as_text = blob.download_as_text()

    return file_as_text


flat_partnerdata_dag:
----------------------
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
	
partnerdata_flat_process.py:
--------------------------
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import pbmis_daily_utils as utils
import logging

if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(description="End-to-End Data Pipeline")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--book_date", required=True, help="Process Booking Date")
    parser.add_argument("--load_user", required=True, help="Load Process ID")
    args = parser.parse_args()
    project_id = args.project_id
    book_date = args.book_date
    load_user = args.load_user

    target_dataset = "ds_pbmis_daily"
    target_table = "t_pbmis_partnerdata"
    flat_query_bucket = "db-dev-europe-west3-gcs-144024-pbmis-dataproc-codebase"#gs://db-dev-europe-west3-gcs-144024-pbmis-dataproc-codebase-ahmed/pbmis-utils/src/poc_ingest_rdp.py
    flat_query_path = "pbmis-utils/src/partnerdata_flat_query.txt"
    temp_gcs_bucket = "db-dev-europe-west3-gcs-144024-pbmis-storage-temp"
    materialization_dataset = "TEST"

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("Executing Partnerdata Flattening job")

    # Create SparkSession
    logger.info("Starting SparkSession creation...")
    try:
        spark = SparkSession.builder \
            .appName("Partnerdata Flattening Process") \
            .getOrCreate()
        logger.info("SparkSession created successfully.")
    except Exception as e:
        logger.error("Failed to create SparkSession: %s", str(e))
        raise
    # Set BigQuery configurations
    spark.conf.set("temporaryGcsBucket", temp_gcs_bucket)
    spark.conf.set("viewsEnabled", "true")
    spark.conf.set("materializationDataset", materialization_dataset)
    logger.info("SparkSession created successfully.")

    # Step 1: Load flattening query from GCS bucket
    logger.info(f"Loading flattening query from GCS : {flat_query_bucket}/{flat_query_path}")
    try:
        flattening_query = utils.read_file_from_gcs(bucket_name=flat_query_bucket, file_path=flat_query_path)
        if not flattening_query.strip():
            logger.error("Flattening query is empty or invalid. Check the file in GCS: %s/%s", flat_query_bucket,flat_query_path)
            raise ValueError("Flattening query is empty or invalid.")
        logger.info("Flattening query loaded successfully.")
    except Exception as e:
        logger.error("Failed to load flattening query: %s", str(e))
        raise

    # Step 2: Execute flattening query
    logger.info(f"Executing flattening query: {flattening_query}")
    try:
        df_flattened = spark.read.format("bigquery").option("query", flattening_query).load()
        logger.info("Flattening query executed successfully. DataFrame created with schema: %s", df_flattened.schema)
    except Exception as e:
        logger.error("Failed to execute flattening query: %s", str(e))
        raise

    # Step 3- Adding Metadata attributes to dataframe
    logger.info(f"Adding static metadata attributes to the DataFrame.")
    static_metadata = {
        "md_book_date": book_date,
        "md_load_date": current_timestamp(),  # Same timestamp for all rows
        "md_load_user": load_user,
        "md_source_system": "partnerdata"
    }
    df_metadata = df_flattened
    for col_name, col_value in static_metadata.items():
        df_metadata = df_metadata.withColumn(col_name, lit(col_value))
        logger.info(f"Added static metadata column '{col_name}' with value '{col_value}'")


    # Step 4- Validating Dataframe Schema vs BQ Target schema
    logger.info(f"Validating DataFrame schema against BigQuery table schema for {target_dataset}.{target_table}.")
    df_final = df_metadata
    valid_schema = utils.validate_schema(df_final, target_dataset, target_table, project_id)


    # Step 5: Write to BigQuery
    if valid_schema:
        logger.info("Schema validation successful.")
        logger.info(f"Writing data to BigQuery table {target_dataset}.{target_table}.")
        try:
            df_final.write.format("bigquery") \
                .option("project", project_id) \
                .option("dataset", target_dataset) \
                .option("table", target_table) \
                .option("partitionField", "md_book_date")\
                .mode("append") \
                .save()
                #.option("writeMethod", "indirect")  Data is first written to the temporary staging place "temp_gcs_bucket"
            logger.info(f"Data successfully written to BigQuery table {target_dataset}.{target_table}.")
        except Exception as e:
            logger.error("Failed to write data to BigQuery: %s", str(e))
            raise
    else:
        logging.info(f"Failed to Write into BQ table {target_dataset}.{target_table} as Schema Validation Failed ")


