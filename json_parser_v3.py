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

def read_json_config(config_file_nm, storage_location):
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
        feed_entity_list = []

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
            logging.info(f"Target table : {target_dataset}.{target_tablename}")
            logging.info(f"Partition Enabled: {partition_enabled}")
            logging.info(f"Partition Columns: {partition_columns}")
            logging.info(f"Cluster Enabled: {cluster_enabled}")
            logging.info(f"Cluster Columns: {cluster_columns}")

            feed_entity_list.append(
                {
                    'process': process_info,
                    'source_system': source_system,
                    'tenant_process': tenant_process,
                    'feed_name': feed_entity_name,
                    'load_type': load_type,
                    'extraction_query': extraction_query,
                    'target_dataset': target_dataset,
                    'target_table': target_tablename,
                    'partition_enabled': partition_enabled,
                    'partition_columns': partition_columns_str,
                    'cluster_enabled': cluster_enabled,
                    'cluster_columns': cluster_columns_str
                }
            )
            """""
            job_config = dict()
            job_config['reference'] = {'project_id': 'project_id'}
            job_config['placement'] = {'cluster_name': 'cluster_name'}
            job_config['pyspark_job'] = {
                "jar_file_uris": ["gs://db-dev-europe-west3-gcs-144024-pbmis-dataproc-codebase-ahmed/jars/spark-bigquery-with-dependencies_2.12-0.34.0.jar"],
                "main_python_file_uri": "gs://db-dev-europe-west3-gcs-144024-pbmis-dataproc-codebase-ahmed/pbmis-utils/src/poc_ingest_rdp.py",
                "args": {
                    "--process_feed": process_info + '_' + source_system + '_' + feed_entity_name,
                    "--src_qry":  extraction_query,
                    "--trgt": target_dataset + '.' + target_table,
                    "--load_typ": load_type,
                    "--partition_enabled": partition_enabled,
                    "--partition_columns": partition_columns_str,
                    "--cluster_enabled": cluster_enabled,
                    "--cluster_columns": cluster_columns_str
                }
            }
            job_list.append({"job_config": job_config, "metadata": metadata})
            """""
        return feed_entity_list

    except Exception as e:
        logging.error(f"Failed to parse the JSON Config File :  {e}")
        return None


def render_sql_query(qry=None, parameters=None):
    rendered_qry = ""
    if qry and parameters:
        temp_qry = Template(qry)
        rendered_qry = temp_qry.render(**parameters)
    return rendered_qry




"""
# Test Example usage
# Import additional libraries for testing
from unittest.mock import MagicMock, patch
# Test Configuration File
TEST_JSON_CONTENT = """
{
  "process_Info": {
    "process_descr": "misDaily",
    "source_system": "PartnerData",
    "tenant_process": "mis-partner-stage",
    "version": "1.0.0"
    },
  "feedEntities": {
    "Partner_Info": {
      "type": "BQ",
      "initial_query": "SELECT h.PARTID, s.MANDANT, s.EROEFFZTPKTPART, s.PARTKLS, s.TSAEND, s.SYS_START, s.SYS_END, s.MD_HDIFF_ATTRIBUTES FROM `rawvault_partnerdata.partner_partner_s` s, `rawvault_partnerdata.partner_partner_h` h WHERE h.MD_HKEY_PARTNER_ID = s.MD_HKEY_PARTNER_ID AND sys_start IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY (h.MD_HKEY_PARTNER_ID) ORDER BY s.TSAEND, s.sys_start, s.sys_end DESC) = 1",
      "delta_query": "SELECT h.PARTID, s.MANDANT, s.EROEFFZTPKTPART, s.PARTKLS, s.TSAEND, s.SYS_START, s.SYS_END, s.MD_HDIFF_ATTRIBUTES FROM `rawvault_partnerdata.partner_partner_s` s, `rawvault_partnerdata.partner_partner_h` h WHERE h.MD_HKEY_PARTNER_ID = s.MD_HKEY_PARTNER_ID AND s.TSAEND > '{{Prev_Working_date}}' QUALIFY ROW_NUMBER() OVER (PARTITION BY (h.MD_HKEY_PARTNER_ID) ORDER BY s.TSAEND, s.sys_start, s.sys_end DESC) = 1",
      "target_dataset": "misdaily_partnerdata",
      "target_tableName": "t_pbmis_partner_stg",
      "partition_enabled": "Yes",
      "partition_columns": ["md_book_date"],
      "cluster_enabled": "Yes",
      "cluster_columns": ["PARTID", "md_book_date"],
      "static_metadata_fields": [
        "md_book_date",
        "md_load_date",
        "md_load_user",
        "md_load_type"
      ]
    },
    "Partner_Natural_Person": {
      "type": "BQ",
      "initial_query": "SELECT h.PARTID, s.MANDANT, s.STANG, s.GEBDAT, s.TSAEND, s.SYS_START, s.SYS_END, s.MD_HDIFF_ATTRIBUTES FROM `rawvault_partnerdata.partner_natural_partner_person_s` s, `rawvault_partnerdata.partner_partner_h` h WHERE h.MD_HKEY_PARTNER_ID = s.MD_HKEY_PARTNER_ID AND sys_start IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY (h.MD_HKEY_PARTNER_ID) ORDER BY s.TSAEND, s.sys_start, s.sys_end DESC) = 1",
      "delta_query": "SELECT h.PARTID, s.MANDANT, s.STANG, s.GEBDAT, s.TSAEND, s.SYS_START, s.SYS_END, s.MD_HDIFF_ATTRIBUTES FROM `rawvault_partnerdata.partner_natural_partner_person_s` s, `rawvault_partnerdata.partner_partner_h` h WHERE h.MD_HKEY_PARTNER_ID = s.MD_HKEY_PARTNER_ID AND s.TSAEND > '{{Prev_Working_date}}' QUALIFY ROW_NUMBER() OVER (PARTITION BY (h.MD_HKEY_PARTNER_ID) ORDER BY s.TSAEND, s.sys_start, s.sys_end DESC) = 1",
      "target_dataset": "misdaily_partnerdata",
      "target_tableName": "t_pbmis_natural_person_stg",
      "partition_enabled": "Yes",
      "partition_columns": ["md_book_date"],
      "cluster_enabled": "Yes",
      "cluster_columns": ["PARTID", "md_book_date"],
      "static_metadata_fields": [
        "md_book_date",
        "md_load_date",
        "md_load_user",
        "md_load_type"
      ]
    }
 }
}
"""
# Write a local test JSON file
TEST_FILE_PATH = "test_config.json"
with open(TEST_FILE_PATH, "w") as test_file:
    test_file.write(TEST_JSON_CONTENT)

# Mock GCS client methods for local testing
with patch("google.cloud.storage.Client") as MockClient:
    # Mock bucket and blob behavior
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_blob.download_as_text.return_value = TEST_JSON_CONTENT
    mock_bucket.blob.return_value = mock_blob
    MockClient().get_bucket.return_value = mock_bucket

    # Test read_json_config
    local_config = read_json_config("test_config.json", "gs://mock-bucket/path/")
    print("\n### Test: read_json_config ###")
    print(local_config)

    # Test render_sql_query
    query = "SELECT * FROM table1 WHERE date > '{{ Prev_Working_date }}'"
    parameters = {"Prev_Working_date": "2024-01-01"}
    rendered_query = render_sql_query(qry=query, parameters=parameters)
    print("\n### Test: render_sql_query ###")
    print(rendered_query)  # Expected: SELECT * FROM table1 WHERE date > '2024-01-01'

    # Test parse_json
    parsed_data = parse_json(local_config, load_type="delta", prev_wrk_dt="2024-01-01")
    print("\n### Test: parse_json ###")
    for item in parsed_data:
        print(item)

# Clean up local test file
os.remove(TEST_FILE_PATH)

"""
