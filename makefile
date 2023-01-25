prepare-install:
	mkdir -p ./dags ./logs ./plugins
	echo "AIRFLOW_UID=50000" > .env


clean:
	docker rm -f $(docker ps -a -q)
init-airflow:
	docker compose up airflow-init
start-airflow:
	docker compose up -d
stop-airflow:
	docker compose down
restart-airflow:
	make stop-airflow
	make start-airflow


build-image-airflow:
	docker build -t airflow-extension:1.0 .
