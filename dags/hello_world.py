"""Hello world."""
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id="hello_world",
    schedule_interval="0 2 * * *",
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

    print_hello >> print_world  # pylint: disable=pointless-statement
