from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

args = {
    "owner": "eb",
}

with DAG(
    dag_id="hello_world_airflow",
    default_args=args,
    schedule_interval="0 5 * * *",
    start_date=pendulum.today("UTC").add(days=-1),
) as dag:

    print_hello = BashOperator(
        task_id="print_hello",
        bash_command="echo Hello",
    )

    print_world = BashOperator(
        task_id="print_world",
        bash_command="echo World",
    )

    print_hello >> print_world

if __name__ == "__main__":
    dag.cli()
