from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud import LocalFilesystemToGCSOperator

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

with DAG(
    dag_id='nist_cve',
    default_args=args,
    schedule_interval='0 5 * * *',
    start_date=days_ago(1),
) as dag:

    # get API data, save locally
    # /tmp/runid/raw.json
    # Note: add a TTL on these buckets that expire much quicker than the later transformations
    # This step will be a cost bottleneck.
    # Each transform becomes more valuable; find ways to lower cost using things like S3 glacier, bucket TTL/expiry, compression
    extract = BashOperator(
        task_id='extract',
        bash_command='echo "curl ..."',
    )

    # TODO LocalFilesystemToGCSOperator
    # /runid/raw.json
    # basically this copies the local API response into the data lake
    # if any of the downstream steps fail, its important to have this source data to retry
    store = BashOperator(
        task_id='store_in_data_lake',
        bash_command='echo "store"',
    )

    # TODO: convert to jsonlines, avro, etc
    # TODO: jq '.result.CVE_Items[].cve.CVE_data_meta.ID'
    # TODO: alternative: https://avro.apache.org/docs/1.10.2/gettingstartedpython.html
    # Note: jq supports streaming!
    # /runid/data.jsonlines
    transform = BashOperator(
        task_id='transform',
        bash_command='echo "convert from raw into a format for data warehouse"'
    )

    # this step moves data from data lake into a place microservice can use
    # gcs to bigtable
    # gcs to bigquery
    # https://pandas.pydata.org/
    # dataflow
    load = BashOperator(
        task_id='load',
        bash_command='echo "load from gcs into data store"'
    )

    extract >> store >> transform >> load

if __name__ == "__main__":
    dag.cli()
