# Local Airflow Setup

Local development environment for Airflow.

## Setup

[install docs](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)

Run `make setup` then `make run`

Navigate to <http://localhost:8080>. Creds: `admin:admin`

## Resources

- [Data Engineering w/ GCP](https://github.com/PacktPublishing/Data-Engineering-with-Google-Cloud-Platform)
- [Docs](https://airflow.apache.org/docs/)

## Research

- [Querying Cloud Storage data](https://cloud.google.com/bigquery/docs/external-data-cloud-storage)
  - [federated queries](https://cloud.google.com/bigquery/docs/federated-queries-intro)
  - [see ETL, EL, and ELT in What is Google BigQuery](https://www.oreilly.com/library/view/google-bigquery-the/9781492044451/ch01.html#what_is_google_bigqueryquestion_mark)
- [Loading JSON data from Cloud Storage](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json#loading_json_data_with_schema_auto-detection)
- [Cloud storage transfers](https://cloud.google.com/bigquery/docs/cloud-storage-transfer)
- [Load data into BigQuery](https://cloud.google.com/bigquery/docs/loading-data)
- [Cloud storage triggers](https://cloud.google.com/functions/docs/calling/storage)
- JSONLines or something like [avro](https://avro.apache.org/), [capacitor](https://cloud.google.com/blog/products/bigquery/inside-capacitor-bigquerys-next-generation-columnar-storage-format)
- [PubSub](https://cloud.google.com/pubsub/architecture)
  - _send over 500 million messages per second_
- [DockerOperator](https://marclamberti.com/blog/how-to-use-dockeroperator-apache-airflow/)
  - code can use poetry & nice tests; won't affect server
  - move to k8s cluster

## Operators

- [airflow.providers.google.cloud.transfers.gcs_to_bigquery](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/transfers/gcs_to_bigquery/index.html)
