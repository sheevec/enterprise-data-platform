.PHONY: setup lint format typecheck test coverage clean

PYTHON ?= python3
VENV ?= .venv

setup:
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -r requirements.txt
	$(VENV)/bin/pre-commit install

format:
	$(VENV)/bin/isort src tests
	$(VENV)/bin/black src tests

lint:
	$(VENV)/bin/isort --check-only src tests
	$(VENV)/bin/black --check src tests
	$(VENV)/bin/flake8 src tests

typecheck:
	$(VENV)/bin/mypy src

test:
	$(VENV)/bin/pytest tests/unit -v

spark-test:
	JAVA_HOME=$$(/usr/libexec/java_home -v 17) $(VENV)/bin/pytest tests/unit/test_bronze_streaming.py -v

# Local/dev submit of the Bronze streaming job. On Dataproc use:
#   gcloud dataproc jobs submit pyspark src/processing/bronze_streaming.py \
#     --cluster=<cluster> --region=<region> \
#     --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-*.jar \
#     --packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-avro_2.12:3.4.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11
bronze-submit-local:
	$(VENV)/bin/spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-avro_2.12:3.4.1 \
		--conf spark.pyspark.python=$(VENV)/bin/python \
		src/processing/bronze_streaming.py

silver-submit-local:
	$(VENV)/bin/spark-submit \
		--packages io.delta:delta-core_2.12:2.4.0 \
		--conf spark.pyspark.python=$(VENV)/bin/python \
		src/processing/bronze_to_silver.py

optimize-submit-local:
	$(VENV)/bin/spark-submit \
		--packages io.delta:delta-core_2.12:2.4.0 \
		--conf spark.pyspark.python=$(VENV)/bin/python \
		src/maintenance/table_optimization.py

coverage:
	$(VENV)/bin/pytest tests/unit --cov=src --cov-report=term-missing --cov-report=html

clean:
	rm -rf .pytest_cache .mypy_cache .coverage htmlcov dist build *.egg-info
	find . -type d -name "__pycache__" -exec rm -rf {} +
