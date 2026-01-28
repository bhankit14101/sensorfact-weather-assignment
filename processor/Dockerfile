FROM apache/spark:3.4.1
USER root
# Install python dependencies for Spark
RUN pip install pyspark
WORKDIR /app
COPY main.py .
# Launch spark-submit with the necessary Kafka and Postgres connectors
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0", "main.py"]
