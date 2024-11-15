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

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--src_qry')
    parser.add_argument('--trgt')
    parser.add_argument('--load_typ')

    args = parser.parse_args()
    sql = args.src_qry
    target = args.trgt
    load_typ = args.load_typ

    logging.info('*****************************')
    logging.info(f"Load Type: {load_typ}")
    logging.info(f"Extraction Query :  {sql}")
    logging.info(f"Target Table: {target}")
    logging.info('*****************************')



    spark = SparkSession.builder.appName("Ingest_RDP_DATA").getOrCreate()
    spark.conf.set('temporaryGcsBucket', "db-dev-europe-west3-gcs-144024-pbmis-storage-temp")
    spark.conf.set("viewsEnabled", "true")
    spark.conf.set("materializationDataset", "TEST")


    df = spark.read.format('bigquery').load(sql)
    if load_typ == 'initial':
        # Call the validate_schema function as part of your job
        result = validate_schema(df, 'my_dataset', 'my_table', project_id='my_project_id')
        # If validation ran, upload logs to a GCS bucket
        if result == True:
            df.write.format('bigquery').option('table', target).mode('overwrite').save()
            logging.info("Data Overwritten Successfully")
    elif load_typ == 'delta':
        df.write.format('bigquery').option('table', target).mode('append').save()
        logging.info("Data Appended Successfully")
    else:
        logging.info("Invalid Load Type")
