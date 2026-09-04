FROM tabulario/spark-iceberg:latest

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY config/spark-defaults.conf /opt/spark/conf/spark-defaults.conf