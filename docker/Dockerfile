FROM apache/airflow:2.9.2
USER airflow
COPY requirements.txt requirements.txt
COPY profiles.yml /home/airflow/.dbt/profiles.yml
RUN pip install -r requirements.txt