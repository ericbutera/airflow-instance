# Local Airflow Setup

Local development environment for Airflow.

## Setup

[install docs](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)

Run `make setup` then `make run`

Navigate to <http://localhost:8080>. Creds: `admin:admin`

## Resources

- [Data Engineering w/ GCP](https://github.com/PacktPublishing/Data-Engineering-with-Google-Cloud-Platform)
- [Data Pipelines w/ Airflow](https://github.com/BasPH/data-pipelines-with-apache-airflow)
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
- [Running tasks in containers 10.3.2](https://livebook.manning.com/book/data-pipelines-with-apache-airflow/chapter-10/v-4/)
  - easier dep management
  - uniform approach for different tasks
  - improved testability
- [airflow go client](https://github.com/apache/airflow-client-go)
  - if running in container, any language is possible
  - from scratch extremely small binaries!
  - can integrate with airflow via OpenAPI spec

## Operators

- [airflow.providers.google.cloud.transfers.gcs_to_bigquery](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/transfers/gcs_to_bigquery/index.html)

## Hello World Go DAG

In order to get docker operator working on Mac, some work-arounds were employed. The docker socket needed to be [wrapped in a service](https://onedevblog.com/how-to-fix-a-permission-denied-when-using-dockeroperator-in-airflow/).

![Successful Run](/docs/hello_world_go_run.png)
