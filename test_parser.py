import json
import logging
from jinja2 import Template

def parse_json(json_config, load_type):
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

            extraction_query = render_sql_query(qry=extraction_query, parameters={'Prev_Working_date': '2023-10-01'})

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
                    "--trgt": target_dataset + '.' + target_tablename
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

# Example usage:
with open('partnerData_config.json', 'r') as f:
    json_data = json.load(f)

load_type = 'delta'  # or 'delta'
job_list = parse_json(json_data, load_type)
logging.info(f"Config content: {json_data}")
if job_list:
    for job in job_list:
        # Print job configurations for validation
        print(f"Job Reference: {job['metadata']}")
        print(f"Target Table: {job['job_config']['pyspark_job']['args']['--trgt']}")
        print(f"Extraction Query: {job['job_config']['pyspark_job']['args']['--src_qry']}")
else:
    print("Failed to parse JSON configuration!")

