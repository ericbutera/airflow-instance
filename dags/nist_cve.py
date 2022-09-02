import tempfile
import datetime
import json
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# https://github.com/apache/airflow/tree/main/airflow/providers/google/cloud/example_dags
#
# TODO: Replace hard-coded paths with `airflow.models.Variable` or xcom
# TODO: full vs incremental
# TODO: its possible to pass config variables into the DAG, like last run date
# TODO validation at each step, do not proceed
# TODO gcp https://github.com/BasPH/data-pipelines-with-apache-airflow/blob/master/chapter18/dags/gcp.py
# TODO gcp https://cloud.google.com/composer/docs/how-to/using/writing-dags
# TODO testing https://medium.com/@jw_ng/writing-unit-tests-for-an-airflow-dag-78f738fe6bfc
# TODO observability https://docs.datadoghq.com/integrations/airflow/?tab=host
#
# Concerns
# - DAG updating
# - DAG unit tests
# - DAG execution environment - prevent OOM, prevent airflow server slowing down
# - Data format
# - Update frequency
# - Retry
# - Rate limits: Token bucket
# - Quality and integrity: Deduplicate, validate, normalize (eg: urlparams in same order)
# - Use hooks over operators
# - Secrets manager (for keys)
# - extraction https://www.tornadoweb.org/en/stable/
# - cloud storage mounts buckets as filesystem using FUSE; useful for the raw & jsonlines stages
# - error tolerances
#   - how many validation errors are acceptable before entire run is cancelled?
#   - failure threshold for: storage, transform, load

args = {
    'owner': 'eb',
}

def _json_load(path):
    with open(path, "r") as file:
        return json.load(file)

gcp_project_id = os.environ.get('NISTY_GCP_PROJECT')
gcp_data_set = os.environ.get('NISTY_GCP_DATA_SET')
bucket = os.environ.get('NISTY_GCP_BUCKET')
table_id = f"{gcp_project_id}.{gcp_data_set}.cves"
table_schema = _json_load("/schemas/nist_cve.json")

def _transform(**context):
    # TODO: fix data_path
    # TODO: harden against errors
    # TODO: stream, not bulk load
    # TODO: run in resource constrained env
    context['data_path'] = '/data'
    path = context["data_path"] + "/" + context["run_id"] + "/raw.json"
    data = _json_load(path)

    out_path = context["data_path"] + "/" + context["run_id"] + "/data.jsonlines"
    with open(out_path, "w") as out:
        for cve in data["result"]["CVE_Items"]:
            row = json.dumps(cve)
            out.write(row + "\n")
with DAG(
    dag_id='nist_cve',
    default_args=args,
    # schedule_interval='0 5 * * *',
    start_date=days_ago(1),
    user_defined_macros={
        "bucket": bucket,
        "data_path": "/data",
        "nist_cve_endpoint": "http://sample-data:8081/nist_cve_full.json"
        # "nist_cve_endpoint": "https://services.nvd.nist.gov/rest/json/cves/1.0/"
    }
) as dag:
    prepare = BashOperator(
        task_id='prepare',
        bash_command="mkdir -p {{ data_path }}/{{run_id}}"
    )

    # get API data, save locally
    # /data/runid/raw.json
    # Note: add a TTL on these buckets that expire much quicker than the later transformations
    # This step will be a cost bottleneck.
    # Each transform becomes more valuable; find ways to lower cost using things like S3 glacier, bucket TTL/expiry, compression
    extract = BashOperator(
        task_id='extract',
        bash_command='curl {{ nist_cve_endpoint }} --output {{ data_path }}/{{ run_id }}/raw.json',
    )

    # basically this copies the local API response into the data lake
    # if any of the downstream steps fail, its important to have this source data to retry
    # Note: use storage exiry and governance
    store_raw = LocalFilesystemToGCSOperator(
        task_id="store_raw",
        src="{{ data_path }}/{{ run_id }}/raw.json",
        dst="{{ run_id }}/raw.json",
        bucket="{{ bucket }}",
    )

    # GoogleCloudStorageToGoogleCloudStorageOperator
    # TODO: convert to jsonlines, avro, etc
    # TODO: jq '.result.CVE_Items[].cve.CVE_data_meta.ID'
    #   Note: jq supports streaming!
    # TODO: alternative: https://avro.apache.org/docs/1.10.2/gettingstartedpython.html
    # TODO: Move this to a cluster with explicit resource limits.
    #   PythonOperators are a bad idea, can easily lead to OOM situations
    transform = PythonOperator(
        task_id='transform',
        python_callable=_transform,
        dag=dag
    )

    store_transform = LocalFilesystemToGCSOperator(
        task_id="store_transform",
        # src="{{ data_path }}/{{ run_id }}.json",
        src="{{ data_path }}/{{ run_id }}/data.jsonlines",
        dst="{{ run_id }}/lines.jsonlines",
        bucket="{{ bucket }}",
    )

    # Generate schema with:
    # bq show --schema --format=prettyjson myprojectid:mydataset.mytable > schemas/nist_cve.json
    load = GoogleCloudStorageToBigQueryOperator(
        task_id                             = "datalake_to_bigquery",
        bucket                              = "{{ bucket }}",
        source_objects                      = ["{{ run_id }}/*.jsonlines"],
        destination_project_dataset_table   = table_id,
        source_format                       = "NEWLINE_DELIMITED_JSON",
        write_disposition                   = 'WRITE_TRUNCATE',
        # autodetect                        = True,
        schema_fields                       = table_schema,
    )

    prepare >> \
    extract >> \
    store_raw >> \
    transform >> \
    store_transform >> \
    load

if __name__ == "__main__":
    dag.cli()
