FROM apache/airflow:2.3.0
COPY --chown=airflow requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt