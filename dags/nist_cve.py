"""NIST CVE demo DAG"""
import os
import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
import pendulum

# Use
# 1. create a connection called `google_cloud_default` for GCP in
#    http://localhost:8080/connection/list/
# 2. Ensure ENV variables are set in .env
#
# https://airflow.apache.org/docs/apache-airflow/2.2.4/best-practices.html#writing-a-dag
#
# Google Operator examples:
# https://github.com/apache/airflow/tree/main/airflow/providers/google/cloud/example_dags
#
# - Replace hard-coded paths with `airflow.models.Variable` or xcom
# - full vs incremental
# - its possible to pass config variables into the DAG, like last run date
# - validation at each step, do not proceed
# - gcp https://cloud.google.com/composer/docs/how-to/using/writing-dags
# - testing https://medium.com/@jw_ng/writing-unit-tests-for-an-airflow-dag-78f738fe6bfc
# - observability https://docs.datadoghq.com/integrations/airflow/?tab=host
# - lint https://pypi.org/project/pylint-airflow/
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
    "owner": "eb",
}


def _json_load(filename):
    with open(filename) as f:
        return json.load(f)


gcp_project_id = os.environ.get("NISTY_GCP_PROJECT")
gcp_data_set = os.environ.get("NISTY_GCP_DATA_SET")
bucket = os.environ.get("NISTY_GCP_BUCKET")
table_id = f"{gcp_project_id}.{gcp_data_set}.cves"
table_schema = _json_load("/schemas/nist_cve.json")


def _transform(**context):
    # - fix data_path
    # - harden against errors
    # - stream, not bulk load
    # - run in resource constrained env
    context["data_path"] = "/data"
    path = context["data_path"] + "/" + context["run_id"] + "/raw.json"
    data = _json_load(path)

    out_path = context["data_path"] + "/" + context["run_id"] + "/data.jsonlines"
    with open(out_path, "w") as out:
        for cve in data["result"]["CVE_Items"]:
            row = json.dumps(cve)
            out.write(row + "\n")  #


with DAG(
    dag_id="nist_cve",
    default_args=args,
    # schedule_interval='0 5 * * *',
    start_date=pendulum.today("UTC").add(days=-1),
    user_defined_macros={
        "bucket": bucket,
        "data_path": "/data",
        "nist_cve_endpoint": "http://sample-data:8081/nist_cve_full.json"
        # "nist_cve_endpoint": "https://services.nvd.nist.gov/rest/json/cves/1.0/"
    },
) as dag:
    prepare = BashOperator(
        task_id="prepare",
        bash_command="mkdir -p {{ data_path }}/{{run_id}}",
    )
    # prepare = DummyOperator(task_id='prepare')

    # get API data, save locally
    # Note: add a TTL on these buckets that expire much quicker than the later transformations (save $)
    # Each transform becomes more valuable; use cost-saving measures like S3 glacier, bucket TTL/expiry, compression
    extract = BashOperator(
        task_id="extract",
        bash_command="curl {{ nist_cve_endpoint }} --output {{ data_path }}/{{ run_id }}/raw.json",
    )
    # extract = DummyOperator(task_id='extract')

    # basically this copies the local API response into the data lake
    # if any of the downstream steps fail, its important to have this source data to retry
    store_raw = LocalFilesystemToGCSOperator(
        task_id="store_raw",
        src="{{ data_path }}/{{ run_id }}/raw.json",
        dst="{{ run_id }}/raw.json",
        bucket="{{ bucket }}",
    )
    # store_raw = DummyOperator(task_id="store_raw")

    # Transform from raw into JSONLines (required for BigQuery load)
    # Alternatives:
    # - (streaming) jq '.result.CVE_Items[].cve.CVE_data_meta.ID'
    # - avro https://avro.apache.org/docs/1.10.2/gettingstartedpython.html
    #
    # - Move this to a cluster with explicit resource limits.
    # - PythonOperators are a bad idea, can easily lead to OOM situations
    transform = PythonOperator(
        task_id="transform",
        python_callable=_transform,
        dag=dag,
    )
    # transform = DummyOperator(task_id='transform')

    store_transform = LocalFilesystemToGCSOperator(
        task_id="store_transform",
        src="{{ data_path }}/{{ run_id }}/data.jsonlines",
        dst="{{ run_id }}/lines.jsonlines",
        bucket="{{ bucket }}",
    )
    # store_transform = DummyOperator(task_id="store_transform")

    # Generate schema with:
    # bq show --schema --format=prettyjson myprojectid:mydataset.mytable > schemas/nist_cve.json
    load = GoogleCloudStorageToBigQueryOperator(
        task_id="load",
        bucket="{{ bucket }}",
        source_objects=["{{ run_id }}/*.jsonlines"],
        destination_project_dataset_table=table_id,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=table_schema,
    )
    # load = DummyOperator(task_id="load")

    prepare >> extract >> store_raw >> transform >> store_transform >> load

if __name__ == "__main__":
    dag.cli()
