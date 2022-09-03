"""Scanner"""
import datetime
from time import sleep
from airflow.decorators import dag, task, task_group
import pendulum


@dag(
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def scanner():
    """Example Scanner"""

    @task
    def start():
        print("start")

    @task
    def end():
        sleep(5)
        print("end")

    @task
    def ec2():
        sleep(30)
        print("ec2")

    @task
    def api_gateway1():
        sleep(30)
        print("api_gateway")

    @task
    def api_gateway2():
        sleep(30)
        print("api_gateway2")

    @task
    def ecs():
        sleep(30)
        print("ecs")

    @task
    def elb1():
        sleep(30)
        print("elb1")

    @task
    def elb2():
        sleep(30)
        print("elb2")

    @task_group
    def elb():
        elb1()
        elb2()

    @task
    def rds():
        sleep(30)
        print("rds")

    @task
    def route53_domains():
        sleep(30)
        print("route53_domains")

    @task
    def route53_zones():
        sleep(30)
        print("route53_zones")

    @task
    def s3():
        sleep(30)
        print("s3")

    @task_group
    def api_gateway():
        api_gateway1()
        api_gateway2()

    @task_group
    def route53():
        route53_domains()
        route53_zones()

    @task_group
    def extract_group():
        api_gateway()
        ec2()
        ecs()
        elb()
        s3()
        rds()
        route53()

    start() >> extract_group() >> end()


dag = scanner()
