
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'eb',
}

with DAG(
    dag_id='hello_world_go',
    description="Hello world via Docker executing golang program.",
    default_args=args,
    schedule_interval='0 5 * * *',
    start_date=days_ago(1),
) as dag:

    hello_bash = BashOperator(
        task_id='print_hello',
        bash_command='echo Hello',
    )

    hello_go = DockerOperator(
        # TODO: make configurable
        task_id='hello_world_go',
        image="ericbutera/airflow-hello-world-go:0.0.1",
        network_mode="bridge",
        # cludy hack https://onedevblog.com/how-to-fix-a-permission-denied-when-using-dockeroperator-in-airflow/
        docker_url="tcp://docker-proxy:2375",
    )

    hello_bash >> hello_go


if __name__ == "__main__":
    dag.cli()
