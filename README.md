# Trefle-api-airflow-vaccum

<!-- ABOUT THE PROJECT -->
### About the repo

This is a project that showcases how to vaccum data from the trefle api using airflow as an orchestrator.

### Prerequisites

* python (v 3.10 or later)
* docker installed
* snowflake account
* aws account
* makefile installed

### Setup

1. Prepare folders and config:
```sh
make prepare-install
```
2. Create image extension:
```sh
make build-image-airflow
```
3. Initiate airflow database:
```sh
make init-airflow
```
4. Start airflow:
```sh
make start-airflow
```
5. Define the needed airflow connection and variables
6. Run the dag ingest_plants_data_to_snowflake_dag 

Other make commands are also available.
