from airflow import DAG
from airflow import models
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from google.cloud import storage
from google.cloud import bigquery
import json
import datetime
from datetime import datetime
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocSubmitSparkJobOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator
)
import json_parser_v2
import pbmis_daily_utils as utils

# Example usage
project_id = "target-project-439512"
region = "europe-west3"  # europe-west3 ->frankfurt
cluster_name = "mis-cluster"
master_type = "n1-standard-2"
worker_type = "n1-standard-2"
num_master = 1
master_disk_type = 'pd-standard'
master_disk_size = 32
num_workers = 2  # Adjust based on workload
worker_disk_type = 'pd-standard'
worker_disk_size = 32
image = "2.0.56-debian10"
service_account = "mis-dataproc-sa@target-project-439512.iam.gserviceaccount.com"
service_account_scope = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/cloud.useraccounts.readonly',
    'https://www.googleapis.com/auth/devstorage.read_write',
    'https://www.googleapis.com/auth/logging.write',
    'https://www.googleapis.com/auth/monitoring.write',
    'https://www.googleapis.com/auth/sqlservice.admin'
]
subnetwork = ""
kms_key = ""
storage_bucket = ""
properties = {
    "dataproc:dataproc.allow.zero.workers": "true",
    "dataproc:dataproc.logging.stackdriver.enable": "true",
    "dataproc:dataproc.logging.stackdriver.job.driver.enable": "true",
    "dataproc:dataproc.logging.stackdriver.job.yarn.container.enable": "true",
    "dataproc:dataproc.jobs.file-backed-output.enable": "true"
}
write_data_to_bigquery = ""

cluster_config = ClusterGenerator(
    cluster_name=cluster_name,
    project_id=project_id,
    num_masters=num_master,
    master_machine_type=master_type,
    master_disk_type=master_disk_type,
    master_disk_size=master_disk_size,
    num_workers=num_workers,
    worker_machine_type=worker_type,
    worker_disk_type=worker_disk_type,
    worker_disk_size=worker_disk_size,
    image_version=image,
    service_account=service_account,
    service_account_scopes=service_account_scope,
    customer_managed_key=kms_key,
    storage_bucket=storage_bucket,
    properties=properties
).make()

cluster_config['gce_cluster_config']['shielded_instance_config'] = {
    'enable_secure_boot': True,
    'enable_vtpm': True,
    'enable_integrity_monitoring': True
}

defualt_dag_args = {
    'owner': 'Composer',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG(
        dag_id='dynamic_partnerdata_pipeline',
        default_args=defualt_dag_args,
        schedule_interval=None,  # Trigger manually or by a scheduler
        catchup=False,
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster_" + cluster_name,
        project_id=project_id,
        cluster_name=cluster_name,
        cluster_config=cluster_config,
        region=region,
        dag=dag
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster_" + cluster_name,
        project_id=project_id,
        cluster_name=cluster_name,
        region=region,
        dag=dag
    )

    # Parsing config and creating tasks within DAG context
    config = utils.read_json_config("partnerdata_config_v3.json",
                                     "gs://europe-west3-pbmis-composer-e4c6b450-bucket/config")
    feed_entity_list = utils.parse_json(config, load_type="delta", prev_wrk_dt="2020-01-01")

    # Initialize a list to store task groups
    task_groups = []
    for feed_entity in feed_entity_list:
        group_id = f"{feed_entity['process']}_{feed_entity['source_system']}_{feed_entity['tenant_process']}_{feed_entity['feed_name']}"
        task_id = f"spark_job_{feed_entity['feed_name']}"

        # Create job configuration
        job_config = dict()
        job_config["reference"] = {'project_id': project_id}
        job_config['placement'] = {'cluster_name': cluster_name}
        job_config['pyspark_job'] = {
            "jar_file_uris": [
                "gs://db-dev-europe-west3-gcs-144024-pbmis-dataproc-codebase-ahmed/jars/spark-bigquery-with-dependencies_2.12-0.34.0.jar"],
            "main_python_file_uri": "gs://db-dev-europe-west3-gcs-144024-pbmis-dataproc-codebase-ahmed/pbmis-utils/src/poc_ingest_rdp.py",
            "args": {
                "--process_feed": f"{feed_entity['process']}_{feed_entity['source_system']}_{feed_entity['feed_name']}",
                "--src_qry": feed_entity['extraction_query'],
                "--trgt": f"{feed_entity['target_dataset']}.{feed_entity['target_table']}",
                "--load_typ": feed_entity['load_type'],
                "--partition_enabled": feed_entity['partition_enabled'],
                "--partition_columns": feed_entity['partition_columns'],
                "--cluster_enabled": feed_entity['cluster_enabled'],
                "--cluster_columns": feed_entity['cluster_columns']
            }
        }
        with TaskGroup(group_id=group_id) as task_group:
            spark_job = DataprocSubmitJobOperator(
                task_id=task_id,
                project_id=project_id,
                region=region,
                job=job_config
            )
        task_groups.append(task_group)

    # Task dependencies
    create_cluster >> task_groups >> delete_cluster



