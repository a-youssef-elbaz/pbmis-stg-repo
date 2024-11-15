import json
import logging
from google.cloud import storage
from google.oauth2 import service_account
import os
from jinja2 import Template

# Set up logging
logging.basicConfig(
    filename='JSON_Parser.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s')

def read_config(config_file_nm, storage_location):
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

        # Parse the JSON content
        json_dict = json.loads(config_file)
        return json_dict

    except Exception as e:
        logging.error(f"Failed to read the config file: {e}")
        return None


def parse_json(json_config, load_type, prev_wrk_dt):
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
            #extraction_query = render_sql_query(qry=extraction_query, parameters={'Prev_Working_date': '2023-10-01'})


            logging.info(f"Extraction Query: {extraction_query}")
            logging.info(f"Target table : {target_dataset}'.'{target_tablename}")

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
                    "--load_typ": load_type
                }
            }
            job_list.append({"job_config": job_config, "metadata": metadata})
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


# Test Example usage
# path of cloud storage service account key
#service_account_key = "C:/Users/ahmed/Downloads/Source_GCS.json"
# Set the environment variable in Python
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key
#config = read_config("partnerData_config.json", "gs://bank_dataset_1/config/data-extraction")
#parse_json(config, "delta")
#logging.info(f"Config content: {config}")

