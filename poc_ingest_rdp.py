from google.cloud import bigquery
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DataType, LongType
from pyspark.sql.functions import *
import json
import datetime
import time
import sys
import os
from google.auth import default
import logging
import pbmis_daily_utils as utils

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--process_feed')
    parser.add_argument('--src_qry')
    parser.add_argument('--trgt')
    parser.add_argument('--load_typ')
    parser.add_argument('--partition_enabled')
    parser.add_argument('--partition_columns')
    parser.add_argument('--cluster_enabled')
    parser.add_argument('--cluster_columns')

    args = parser.parse_args()
    process_feed = args.process_feed
    sql = args.src_qry
    target = args.trgt
    load_typ = args.load_typ
    partition_enabled = args.partition_enabled.lower() == 'yes'
    partition_columns = args.partition_columns.split(',') if args.partition_columns else []
    cluster_enabled = args.cluster_enabled.lower() == 'yes'
    cluster_columns = args.cluster_columns.split(',') if args.cluster_columns else []

    source_project = ""
    target_project = ""
    book_date = ""
    process_name = ""
    load_user = ""

    logging.info('*****************************')
    logging.info(f"Target Table: {target}")
    logging.info(f"Load Type: {load_typ}")
    logging.info(f"Extraction Query :  {sql}")
    logging.info(f"Partition Enabled: {partition_enabled}")
    logging.info(f"Partition Columns: {partition_columns}")
    logging.info(f"Cluster Enabled: {cluster_enabled}")
    logging.info(f"Cluster Columns: {cluster_columns}")
    logging.info('*****************************')

    # 1- Creating Spark session
    spark = utils.create_spark_session(
        app_name=process_feed,
        temp_gcs_bucket="db-dev-europe-west3-gcs-144024-pbmis-storage-temp",
        views_enabled=True,
        materialization_dataset="TEST"
    )
    #spark = SparkSession.builder.appName(process_feed).getOrCreate()
    #spark.conf.set('temporaryGcsBucket', "db-dev-europe-west3-gcs-144024-pbmis-storage-temp")
    #spark.conf.set("viewsEnabled", "true")
    #spark.conf.set("materializationDataset", "TEST")

    # 2- Extracting the source data
    extraction_df = utils.load_data_from_bigquery(spark, project_id=source_project, sql=sql)
    # df = spark.read.format('bigquery').load(sql)

    # 3- Adding Metadata attributes to dataframe
    metadata = {}
    metadata = utils.prepare_static_metadata(book_date, process_name, load_user)
    extraction_df = utils.add_static_metadata(metadata)

    # 4- Validating Dataframe Schema vs BQ Target schema
    dataset_nm, table_nm = target.split('.')
    valid_schema = utils.validate_schema(extraction_df, dataset_nm, table_nm, target_project)

    # 5- Loading data into target BQ table
    if valid_schema:
        utils.write_to_bigquery(extraction_df, dataset_nm, table_nm, load_typ, target_project,
                                partition_columns=partition_columns, cluster_columns=cluster_columns)
    else:
        logging.info(f"Failed to Write into BQ table {target} as Schema Validation Failed ")

"""
    if load_typ == 'initial':
        # Call the validate_schema function as part of your job
        result = utils.validate_schema(df, 'my_dataset', 'my_table', project_id='my_project_id')
        # If validation ran, upload logs to a GCS bucket
        if result == True:
            df.write.format('bigquery').option('table', target).mode('overwrite').save()
            logging.info("Data Overwritten Successfully")
    elif load_typ == 'delta':
        df.write.format('bigquery').option('table', target).mode('append').save()
        logging.info("Data Appended Successfully")
    else:
        logging.info("Invalid Load Type") 
 """
