import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, from_json, sum as _sum, explode, from_unixtime, from_utc_timestamp, min as _min, row_number
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, LongType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
PG_URL = os.getenv("POSTGRES_URL")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASS = os.getenv("POSTGRES_PASS")

schema = StructType([
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("location_name", StringType()),
    StructField("minutely", ArrayType(StructType([
        StructField("dt", LongType()),
        StructField("precipitation", DoubleType())
    ])))
])

def process_and_sink(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    latest_updates = batch_df \
        .select(
            "location_name",
            "lat",
            "lon",
            "total_next_hour_mm",
            "kafka_receive_time",
            col("forecast_window_start").cast("timestamp").alias("forecast_window_start")
        )

    # 2. Write to Postgres using Spark JDBC (Append Mode)
    latest_updates.write \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "precipitation_forecasts") \
        .option("user", PG_USER) \
        .option("password", PG_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

def main():
    spark = SparkSession.builder \
        .appName("WeatherProcessor") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0"
                ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")


    raw_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "weather_raw") \
        .load() \
        .select("timestamp", "value")

    parsed_df = raw_df.select(
        col("timestamp").alias("kafka_receive_time"),
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("kafka_receive_time", "data.*")

    exploded_df = parsed_df.withColumn("m", explode(col("minutely")))

    transformed_df = exploded_df.withColumn(
        "forecast_time_cet",
        from_utc_timestamp(from_unixtime(col("m.dt")), "Europe/Amsterdam")
    )

    batch_results = transformed_df.groupBy("kafka_receive_time", "location_name", "lat", "lon") \
        .agg(
            _sum("m.precipitation").alias("total_next_hour_mm"),
            _min("forecast_time_cet").alias("forecast_window_start")
        )

    query = batch_results.writeStream \
        .foreachBatch(process_and_sink) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoints") \
        .start()

# todo: better stateful checkpoint locations. cloud storage/ mounts?
    query.awaitTermination()

if __name__ == "__main__":
    main()
