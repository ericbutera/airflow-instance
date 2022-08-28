.PHONY: setup
setup:
	sh setup.sh

.PHONY: init
init:
	mkdir -p ./dags ./logs ./plugins
	echo -e "AIRFLOW_UID=$(id -u)" > .env
	docker compose up airflow-init

.PHONY: clean
clean:
	docker compose down --volumes --remove-orphans

.PHONY: delete
delete:
	docker compose down --volumes --rmi all

.PHONY: run
run:
	docker compose up
