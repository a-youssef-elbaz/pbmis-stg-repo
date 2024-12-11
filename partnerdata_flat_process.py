import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import pbmis_daily_utils as utils
import logging

if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser()
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


