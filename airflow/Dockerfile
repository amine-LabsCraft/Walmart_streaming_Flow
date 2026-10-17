FROM apache/airflow:3.2.2

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==3.2.2" \
    -r /requirements.txt
