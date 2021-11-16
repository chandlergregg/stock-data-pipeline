# stock-data-pipeline

## Overview

This project is a prototype of a data pipeline that consumes raw stock market data, processes the data using Python and Spark, outputs the data into Azure file storage, and tracks the process using Postgres. While the volume of data consumed in the project is quite low, the pipeline is designed to scale very quickly due to its cloud-based architecture and the use of Spark.

This project is part of the Springboard Data Engineering program curriculum.

## Pipeline steps

Input format: mixture of CSV and JSON event files that log various stock data throughout the trading day.s

1. Ingests data into Spark after parsing CSV / JSON using Python function passed to PySpark
2. Preprocesses data by deduplicating and writing to Spark SQL tables
3. Runs analytic queries on data and outputs

Output format: Parquet file / table containing analytical data

## Architecture

All pipeline coordination is done by Python. Python runs Spark jobs using [Databricks Connect](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect) - future iterations could consider packaging the Python code to be run directly in the Spark cluster and use a separate orchestration technology (e.g. Airflow) to manage pipeline runs.

The Python coordinator writes the status of pipeline steps and runs to Postgres for tracking.

The coordinator doesn't interact with Azure storage - Spark loads and writes data directly from object storage, as would be the case in a Spark-Hadoop or Spark-data lake architecture.

Components:
- Object storage for input / output data: [Azure Storage - Standard general-purpose v2](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-overview)
- Data processing: Spark / PySpark - [Azure Databricks 9.1 LTS](https://azure.microsoft.com/en-us/services/databricks/)
- Database for pipeline tracking: Postgres 11 - [Azure Database for PostgreSQL server](https://azure.microsoft.com/en-us/services/postgresql/)
- Pipeline coordination: Python 3.10 (run locally using [Pipenv](https://docs.pipenv.org/))

## System requirements

Reproducing the project locally requires the cloud-hosted components mentioned above in "Architecture" as well as the following:
- Requirement / dependency / package management:
    - Pipenv (can be installed with `pip`): handles all Python packages and dependencies for project
    - Postgres (requirement for [`psycopg2`](https://www.psycopg.org/), the Python-Postgres driver package. There are ways to install `psycopg2` without a Postgres installation on the local machine, but installing Postgres first is easier.)
    - [Databricks Connect](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect)
- Handled by Pipenv:
    - Python 3.10
    - See Pipfile for detailed list of Python packages that are installed by Pipenv
- Cloud configuration:
    - Config file: needs to be named `config.cfg` - see `example_config.txt` for an example of how this configuration file should look, with fields to be filled in according to the above components

To set up the Python environment for running the code, run the following commands:
```
pipenv install
pipenv shell
```

To reproduce the Postgres tracker, see `postgres_queries.sql` within `pyspark_etl_pipeline` to initialize the database and table referenced by `tracker.py`.

## Project structure

The project is contained in the `pyspark_etl_pipeline` folder. Other folders in the project contain Python files / iPython notebooks used to develop the PySpark code.

`Pipfile` and `Pipfile.lock` are included in the root directoy.

Make sure to create a `config.cfg` file (using `example_config.txt` as template) to connect to Spark, Postgres, and Azure object storage. (The actual config file is not included as it contains identifiers and passwords for the project Azure infrastructure.)
