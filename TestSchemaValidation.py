import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType, IntegerType, FloatType, BooleanType, DateType, TimestampType, DecimalType, StructType, StructField)
from google.cloud import bigquery
import logging


# Mapping BigQuery types to PySpark types
BIGQUERY_TO_PYSPARK_TYPE_MAP = {
    'STRING': StringType,
    'INTEGER': IntegerType,
    'FLOAT': FloatType,
    'DATE': DateType,
    'TIMESTAMP': TimestampType,
    'NUMERIC': DecimalType,
    'BOOLEAN': BooleanType
    # Add more type mappings as necessary
}

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
    logging.basicConfig(level=logging.INFO)
    logging.info("Validating DataFrame schema against BigQuery table")

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



# Your testing goes here

# Set up a basic PySpark session
spark = SparkSession.builder.master("local[1]").appName("Simple Test").getOrCreate()

# Create a mock DataFrame schema
df_schema = StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('birth_date', DateType(), True),
    StructField('birth_country', StringType(), True),
    StructField('profession', StringType(), True)
])
df = spark.createDataFrame([], schema=df_schema)

# Mock the BigQuery client and table schema
with patch('schema_validation.bigquery.Client') as mock_bigquery_client:  # Replace 'your_script_name' with your script filename
    mock_table = MagicMock()
    mock_table.schema = [
        bigquery.SchemaField('name', 'STRING'),
        bigquery.SchemaField('age', 'INTEGER'),
        bigquery.SchemaField('birth_date', 'DATE'),
        bigquery.SchemaField('profession', 'INTEGER')
    ]
    mock_bigquery_client.return_value.get_table.return_value = mock_table

    # Run the validation function
    result = validate_schema(df, 'test_project', 'test_dataset', 'test_table')

    # Print the result
    if result:
        print("Schema validation passed.")
    else:
        print("Schema validation failed.")