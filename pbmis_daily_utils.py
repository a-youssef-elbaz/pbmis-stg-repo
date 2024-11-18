import json
import logging
from google.cloud import storage
from google.oauth2 import service_account
import os
from jinja2 import Template
import logging
from google.cloud import bigquery
from google.cloud import storage
from pyspark.sql.types import (
    StringType, IntegerType, FloatType, BooleanType, DateType, TimestampType, DecimalType
)
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql import SparkSession

def set_custom_logger(custom_logger=None):
    global LOGGER
    if custom_logger is None:
        # Create a default logger with a specific configuration
        LOGGER = logging.getLogger(__name__)
        handler = logging.StreamHandler()  # Output logs to the console
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        LOGGER.addHandler(handler)
        LOGGER.setLevel(logging.INFO)  # Set a default logging level
    else:
        # Use the provided custom logger
        LOGGER = custom_logger

    LOGGER.info("set_custom_logger() successfully completed")


def read_json_config(config_file_nm, storage_location):
    logging.debug(f"Attempting to read JSON configuration from: {storage_location}")
    logging.info(f"Storage Location : {storage_location}")

    try:
        # Extract the bucket name & config file directory
        config_path_parts = storage_location[5:].split('/', 1)
        bucket_nm = config_path_parts[0]
        config_path = f"{config_path_parts[1]}/{config_file_nm}" if len(config_path_parts) > 1 else config_file_nm

        # Initialize the GCS client and get the bucket
        client = storage.Client()
        storage_bucket = client.get_bucket(bucket_nm)

        # Read file content from bucket as text
        blob_file = storage_bucket.blob(config_path)
        config_file = blob_file.download_as_text()

        # prepare the JSON content to be python obj
        json_dict = json.loads(config_file)
        LOGGER.info(f"Successfully read JSON configuration file: {config_file_nm} ")
        return json_dict

    except Exception as e:
        logging.error(f"Failed to read the config file: {e}")
        return None


def parse_json(json_config, load_type, prev_wrk_dt):
    logging.info(f"Starting to parse JSON configuration for load type: {load_type}")
    try:
        job_list = []

        # Extracting process info
        process_info = json_config['process_Info']['process_descr']
        tenant_process = json_config['process_Info']['tenant_process']
        source_system = json_config['process_Info']['source_system']
        logging.info(f"Process: {process_info}")
        logging.info(f"Source System: {source_system}")
        logging.info(f"Process Descr: {tenant_process}")

        for feed_entity_name, feed_item in json_config['feedEntities'].items():
            logging.info(f"feed Entity: {feed_entity_name}")
            if load_type == 'initial':
                extraction_query = feed_item['initial_query']
            else:
                extraction_query = feed_item['delta_query']
            target_dataset = feed_item['target_dataset']
            target_tablename = feed_item['target_tableName']
            metadata_attributes = feed_item['static_metadata_fields']

            # render sql query .. replacing placeholders with actual values
            extraction_query = render_sql_query(qry=extraction_query, parameters={'Prev_Working_date': prev_wrk_dt})

            # Extract Partitioning & clustering configurations
            partition_enabled = feed_item.get('partition_enabled', 'No')
            partition_columns = feed_item.get('partition_columns', [])
            cluster_enabled = feed_item.get('cluster_enabled', 'No')
            cluster_columns = feed_item.get('cluster_columns', [])
            # Converting lists to strings for command-line arguments
            partition_columns_str = ','.join(partition_columns)
            cluster_columns_str = ','.join(cluster_columns)

            logging.info(f"Extraction Query: {extraction_query}")
            logging.info(f"Target table : {target_dataset}'.'{target_tablename}")
            logging.info(f"Partition Enabled: {partition_enabled}")
            logging.info(f"Partition Columns: {partition_columns}")
            logging.info(f"Cluster Enabled: {cluster_enabled}")
            logging.info(f"Cluster Columns: {cluster_columns}")

            # process metadata
            metadata = {
                'process': process_info,
                'tenant_process': tenant_process,
                'source_system': source_system,
                'feed': feed_entity_name
            }
            job_config = dict()
            job_config['reference'] = {'project_id': 'project_id'}
            job_config['placement'] = {'cluster_name': 'cluster_name'}
            job_config['pyspark_job'] = {
                "jar_file_uris": ["gs://db-dev-europe-west3-gcs-144024-pbmis-dataproc-codebase-ahmed/jars/spark-bigquery-with-dependencies_2.12-0.34.0.jar"],
                "main_python_file_uri": "gs://db-dev-europe-west3-gcs-144024-pbmis-dataproc-codebase-ahmed/pbmis-utils/src/poc_ingest_rdp.py",
                "args": {
                    "--src_qry":  extraction_query,
                    "--trgt": target_dataset + '.' + target_tablename,
                    "--load_typ": load_type,
                    "--partition_enabled": partition_enabled,
                    "--partition_columns": partition_columns_str,
                    "--cluster_enabled": cluster_enabled,
                    "--cluster_columns": cluster_columns_str
                }
            }
            job_list.append({"job_config": job_config, "metadata": metadata})
        LOGGER.info(f"Successfully parsed JSON configuration for Process: {process_info} - Source System: {source_system} - Process Descr: {tenant_process}")
        return job_list

    except Exception as e:
        logging.error(f"Failed to parse the JSON Config File :  {e}")
        return None


def render_sql_query(qry=None, parameters=None):
    rendered_qry = ""
    if qry and parameters:
        temp_qry = Template(qry)
        rendered_qry = temp_qry.render(**parameters)
    return rendered_qry


def create_spark_session(app_name, temp_gcs_bucket=None, views_enabled=True,
                         materialization_dataset=None):
    logging.info(f"Creating a Spark session with app name: {app_name}")
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    if temp_gcs_bucket:
        spark.conf.set('temporaryGcsBucket', temp_gcs_bucket)
    spark.conf.set("viewsEnabled", str(views_enabled).lower())
    if materialization_dataset:
        spark.conf.set("materializationDataset", materialization_dataset)
    logging.info("Spark Session created successfully")

    return spark

def validate_schema(df, dataset_id, table_id, partitioning_field=None, clustering_fields=None, project_id=None):
    """
    Validates that each field in a PySpark DataFrame exists in the BigQuery table schema.

    Args:
        df: The PySpark DataFrame to validate.
        project_id: The Google Cloud Project ID.
        dataset_id: The BigQuery dataset ID.
        table_id: The BigQuery table ID.

    Returns:
        True if the DataFrame schema matches the BigQuery table schema, False otherwise.
    """
    logging.info(f"Starting Schema Validation of DataFrame against BigQuery table {dataset_id}.{table_id}")

    # Mapping BigQuery types to PySpark types
    BIGQUERY_TO_PYSPARK_TYPE_MAP = {
        'STRING': StringType,
        'INTEGER': IntegerType,
        'FLOAT': FloatType,
        'BOOLEAN': BooleanType,
        'DATE': DateType,
        'TIMESTAMP': TimestampType,
        'NUMERIC': DecimalType
        # Add more type mappings as necessary
    }
    try:
        # Create a BigQuery client & Fetch the target table schema
        if project_id:
            client = bigquery.Client(project=project_id)
            table_ref = f"{project_id}.{dataset_id}.{table_id}"
        else:
            client = bigquery.Client()
            table_ref = f"{dataset_id}.{table_id}"
        table = client.get_table(table_ref)
        target_schema_fields = {field.name: field.field_type for field in table.schema}
        target_partitioning_field = table.partitioning_field
        target_clustering_fields = table.clustering_fields

        # Iterate through the DataFrame schema and check against the BigQuery table schema
        for df_field in df.schema.fields:
            field_name = df_field.name
            df_type = type(df_field.dataType)

            if field_name not in target_schema_fields:
                logging.error(f"Field '{field_name}' in DataFrame not found in BigQuery table")
                return False

            # Map BigQuery type to PySpark type for comparison
            expected_df_type = BIGQUERY_TO_PYSPARK_TYPE_MAP.get(target_schema_fields[field_name])
            if expected_df_type is None or df_type != expected_df_type:
                logging.error(f"Type mismatch for field '{field_name}': expected {target_schema_fields[field_name]}, got {df_type}")
                return False

        # Check if the partitioning field matches with BigQuery schema partitioning
        if target_partitioning_field:
            if target_partitioning_field not in df.columns:
                logging.error(f"BigQuery Partitioning field '{target_partitioning_field}' not found in DataFrame.")
                return False
            elif partitioning_field and partitioning_field != target_partitioning_field:
                logging.error(f"Partitioning field '{partitioning_field}' in DataFrame does not match BigQuery partitioning field '{target_partitioning_field}'.")
                return False
            else:
                logging.info(f"Partitioning field in Dataframe matches with BigQuery partitioning field '{target_partitioning_field}'.")
        elif partitioning_field:
            logging.info(f"BigQuery does not have a partitioning field, but the DataFrame defines '{partitioning_field}'.")

        # Check if the clustering fields match with BigQuery schema clustering fields
        if target_clustering_fields:
            if set(clustering_fields) != set(target_clustering_fields):
                logging.error(f"Clustering fields '{clustering_fields}' in DataFrame do not match BigQuery clustering fields '{target_clustering_fields}'.")
                return False
            else:
                logging.info(f"Clustering fields in DataFrame match BigQuery clustering fields.")
        elif clustering_fields:
            logging.info(f"BigQuery does not have clustering fields, but the DataFrame defines clustering fields.")

        logging.info("Schema validation successful")
        return True

    except bigquery.NotFound:
        logging.error(f"BigQuery table {dataset_id}.{table_id} not found.")
        return False
    except bigquery.BadRequest as e:
        logging.error(f"BadRequest error: {e}")
        return False
    except Exception as e:
        logging.error(f"Failed to validate the Dataframe against BQ table {dataset_id}.{table_id} :  {e}")
        return False


def write_to_bigquery(df, dataset, table, load_typ, project_id=None, partition_columns=None, cluster_columns=None):
    """Writes a DataFrame to a BigQuery table with optional project_id, partitioning and clustering."""

    logging.info(f"Starting  write to BigQuery table: {dataset}.{table}")
    if project_id is None:
        project_id = df.sparkSession.conf.get("spark.dataproc.project.id")
    if not project_id:
        logging.error("Project ID could not be determined from context or provided argument.")

    if load_typ == 'initial':
        mode = 'overwrite'
    else:
        mode = 'append'
    logging.info(f"Load type is: {load_typ}")
    logging.info(f"Write mode set to: {mode}")

    writer = (df.write.format("bigquery")
              .option("project", project_id)
              .option("dataset", dataset)
              .option("table", table)
              .mode(mode))

    if partition_columns:
        writer = writer.option("partitionField", ','.join(partition_columns))
    if cluster_columns:
        writer = writer.option("clusteredFields", ','.join(cluster_columns))

    writer.save()
    logging.info(f"Write operation to BigQuery  table: {dataset}.{table} completed.")



def load_data_from_bigquery(spark, project_id=None, sql=None, dataset=None, table=None):
    """
    Loads data from BigQuery using either a SQL query or table details.

    Args:
        spark: The SparkSession instance to use for loading the data.
        sql: SQL query to fetch the data from BigQuery (optional, should be provided if using SQL query).
        project_id: Google Cloud project ID (optional, required if using table details).
        dataset: BigQuery dataset name (optional, required if using table details).
        table: BigQuery table name (optional, required if using table details).

    Returns:
        A PySpark DataFrame with the data loaded from BigQuery.
    """
    logging.info("Starting load/extract data from bigquery")
    if sql:
        # If SQL query is provided, use the query to load the data
        df = spark.read.format('bigquery').option("project", project_id).option('query', sql).load()
        logging.info(f"Data loaded successfully from BigQuery using SQL query: {sql}")
    elif project_id and dataset and table:
        # If project_id, dataset, and table are provided, use the table details to load the data
        df = spark.read.format("bigquery") \
            .option("project", project_id) \
            .option("dataset", dataset) \
            .option("table", table) \
            .load()
        logging.info(f"Data loaded successfully from BigQuery table: {project_id}.{dataset}.{table}")
    else:
        logging.info("Neither SQL query nor table details (project_id, dataset, and table) provided.")
        raise ValueError("Neither SQL query nor table details (project_id, dataset, and table) provided.")

    return df


def prepare_static_metadata(book_date, process_name, load_user):
    """
    Prepares the static metadata dictionary for adding to a DataFrame.

    Args:
        md_book_date: The md_book_date value passed as a parameter during process execution.
        process_name: The process name in the DAG.
        load_user: The service account used for the process.

    Returns:
        A dictionary with column names as keys and their values to be used in the DataFrame.
    """
    static_metadata = {
        "md_book_date": book_date,
        "md_load_date": current_timestamp(),  # Same timestamp for all rows
        "md_load_user": load_user,
        "md_source_system": process_name
    }
    return static_metadata


def add_static_metadata(df, metadata):
    """
    Adds static metadata to the DataFrame using the provided dictionary of key-value pairs.

    Args:
        df: The PySpark DataFrame to which metadata columns will be added.
        metadata: A dictionary where the key is the column name and the value is the column value.

    Returns:
        DataFrame with additional static metadata columns.
    """
    for col_name, col_value in metadata.items():
        df = df.withColumn(col_name, lit(col_value))
        logging.info(f"Added static metadata column '{col_name}' with value '{col_value}'")
    return df






