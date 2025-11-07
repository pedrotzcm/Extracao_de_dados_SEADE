.PHONY: up down build init user all

IMAGE=airflow_teste

up:
	docker compose up -d

down:
	docker compose down --volumes --remove-orphans || true

build:
	docker build . -t $(IMAGE)

init:
	docker compose run --rm --no-deps airflow-webserver airflow db init

user:
	docker compose run --rm --no-deps airflow-webserver airflow users create \
	  --username pedro --password senha --firstname pedro --lastname cruz \
	  --role Admin --email pedrocm123@usp.br || \
	docker compose run --rm --no-deps airflow-webserver airflow users update-password \
	  --username pedro --password senha

all: down build up init user
