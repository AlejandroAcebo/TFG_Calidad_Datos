FROM apache/airflow:2.9.0-python3.11
USER airflow
RUN pip install -U pip
RUN pip install great_expectations==1.0.2