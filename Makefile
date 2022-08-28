.DEFAULT_GOAL := help

.PHONY: delete
delete: ## Remove all data from the instance
	docker compose down --volumes --rmi all

.PHONY: help
help: ## Help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: init
init: ## Run this first; required to start instance
	mkdir -p ./dags ./logs ./plugins
	echo -e "AIRFLOW_UID=$(id -u)" > .env
	docker compose up airflow-init

.PHONY: run
run: ## Run the airflow instance
	docker compose up

.PHONY: stop
stop: ## Stop the airflow instance
	docker compose down
