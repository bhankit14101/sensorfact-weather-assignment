import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as _sum, explode, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, LongType

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
PG_URL = os.getenv("POSTGRES_URL")
PG_PROPS = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASS"),
    "driver": "org.postgresql.Driver"
}

# Schema for the API response
schema = StructType([
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("location_name", StringType()),
    StructField("minutely", ArrayType(StructType([
        StructField("dt", LongType()),
        StructField("precipitation", DoubleType())
    ])))
])

def write_to_postgres(df, epoch_id):
    # This function is called for every micro-batch
    df.write \
        .jdbc(url=PG_URL, table="precipitation_forecasts", mode="overwrite", properties=PG_PROPS)

def main():
    # Initialize Spark with Kafka and Postgres connectors
    spark = SparkSession.builder \
        .appName("WeatherPrecipitationProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "weather_raw") \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse JSON and transform
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # 3. Explode the minutely array to calculate the sum for the next hour
    exploded_df = parsed_df.withColumn("minute_forecast", explode(col("minutely"))) \
        .select(
            "lat", "lon", "location_name",
            col("minute_forecast.precipitation").alias("precip")
        )

    # 4. Aggregate: Total precipitation for the next 60 minutes
    results_df = exploded_df.groupBy("lat", "lon", "location_name") \
        .agg(_sum("precip").alias("total_next_hour_mm"))

    # 5. Output to Postgres
    query = results_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("complete") \
        .option("checkpointLocation", "/tmp/spark_checkpoints") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()