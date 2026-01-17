DOCKER_GID := $(shell getent group docker | cut -d: -f3)
export DOCKER_GID

build:
	docker compose build

dev-jupyter-up:
	 docker compose up spark-master spark-worker jupyter minio

dev-streaming-up:
	 docker compose up spark-master spark-worker kafka zookeeper minio

dev-scheduler-up:
	 docker compose up airflow-webserver airflow-scheduler redis postgres spark-master spark-worker minio

dev-cdc-up:
	 docker compose up kafka zookeeper debezium postgres 

dev-dbt-up:
	 docker compose up dbt postgres	

dev-elk-up:
	 docker compose -f docker-compose.elk.yml --env-file .env.elasticsearch up -d

dev-elk-down:
	 docker compose -f docker-compose.elk.yml --env-file .env.elasticsearch down

dev-up-all-services:
	 docker compose up airflow-webserver airflow-scheduler redis postgres spark-master spark-worker kafka zookeeper debezium minio dbt
	docker compose up -d

dev-down-all-services:
	docker compose down 

dev-clean-all-services:
	docker compose down -rmi all

dev-build-all-services:
	docker compose down --rmi all -v
	docker compose up --build -d

dev-spark-shell:
	docker exec -it spark-master bash

dev-jupyter-shell:
	docker exec -it spark-jupyter bash 

dev-postgres-shell:
	docker exec -it postgres bash

dev-airflow-init-db:
	docker exec -it airflow-webserver airflow db init

dev-airflow-migrate:
	docker exec -it airflow-webserver airflow db migrate

dev-airflow-create-user:
	docker exec -it airflow-webserver airflow users create \
		--username airflowAdmin \
		--firstname Admin \
		--lastname Admin \
		--role Admin \
		--email admin@example.com
		--password airflow1234

dev-airflow-webserver:
	docker exec -it airflow-webserver bash

dev-airflow-scheduler:
	docker exec -it airflow-scheduler bash

dev-redis:
	docker exec -it redis bash

dev-airflow-dags-list:
	docker exec -it airflow-webserver airflow dags list

dev-kafka-logs:
	docker logs kafka

dev-spark-logs:
	docker logs spark-master

dev-spark-worker-logs:
	docker logs spark-worker

dev-airflow-logs:
	docker logs airflow-webserver

dev-postgres-logs:
	docker logs postgres

dev-kafka-data-clean: 
	rm -rf kafka-data/*

dev-dbt-shell:
	docker exec -it dbt bash

dev-dbt-debug:
	docker exec -it dbt dbt debug
# ClickHouse targets for gold layer analytics
dev-clickhouse-up:
	docker compose up -d clickhouse

dev-clickhouse-shell:
	docker exec -it clickhouse clickhouse-client -u default -p clickhousepass

dev-clickhouse-logs:
	docker logs clickhouse

dev-clickhouse-down:
	docker compose down clickhouse

dev-dbt-run-gold:
	docker exec -it dbt bash -c "cd /opt/airflow/sap_dbt && dbt run --select tag:gold --target clickhouse"

dev-dbt-test-gold:
	docker exec -it dbt bash -c "cd /opt/airflow/sap_dbt && dbt test --select tag:gold --target clickhouse"

dev-dbt-docs-gold:
	docker exec -it dbt bash -c "cd /opt/airflow/sap_dbt && dbt docs generate --target clickhouse"