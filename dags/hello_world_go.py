"""Hello World Go"""
import pendulum

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="hello_world_go",
    description="Hello world via Docker executing golang program.",
    schedule_interval="0 1 * * *",
    start_date=pendulum.today("UTC").add(days=-1),
) as dag:
    hello_go = DockerOperator(
        # make configurable
        task_id="hello_go",
        image="ericbutera/airflow-hello-world-go:0.0.1",
        network_mode="bridge",
        # cludy hack
        # https://onedevblog.com/how-to-fix-a-permission-denied-when-using-dockeroperator-in-airflow/
        docker_url="tcp://docker-proxy:2375",
    )

    hello_go  # pylint: disable=pointless-statement
