import logging
from google.cloud import bigquery
from google.cloud import storage
from pyspark.sql.types import (
    StringType, IntegerType, FloatType, BooleanType, DateType, TimestampType, DecimalType
)

# Configure a separate logger for schema validation
logger = logging.getLogger('schema_validation')
handler = logging.FileHandler('Schema_Validation.log')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def validate_schema(df, dataset_id, table_id, project_id=None):
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

        logging.info("Schema validation successful")
        return True

    except bigquery.NotFound:
        logging.error(f"BigQuery table {dataset_id}.{table_id} not found.")
        return None
    except bigquery.BadRequest as e:
        logging.error(f"BadRequest error: {e}")
        return None
    except Exception as e:
        logging.error(f"Failed to validate the Dataframe against BQ table {dataset_id}.{table_id} :  {e}")
        return None


# Helper function to upload logs to GCS
def upload_logs_to_gcs(bucket_name, blob_name, local_log_path='/tmp/Schema_Validation.log'):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_log_path)
    logging.info(f"Log file uploaded to gs://{bucket_name}/{blob_name}")