.PHONY: setup
setup:
	sh setup.sh

.PHONY: clean
clean:
	docker-compose down --volumes --remove-orphans

.PHONY: delete
delete:
	docker-compose down --volumes --rmi all

.PHONY: run
run:
	docker-compose up