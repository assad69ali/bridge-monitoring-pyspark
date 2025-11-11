import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_timestamp, to_date, lit


def main(run_seconds: int = 30):
    """
    Start bronze streaming for a limited time (default 30 seconds),
    then stop all queries and shut down Spark.
    """
    spark = (
        SparkSession.builder
        .appName("BridgeBronzeIngest")
        .config("spark.hadoop.io.nativeio.useNativeIO", "false")
        .getOrCreate()
    )

    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("bridge_id", IntegerType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("ingest_time", StringType(), True),
    ])

    # Colab working dir is /content, so "streams/..." = /content/streams/...
    temp_raw = (
        spark.readStream
        .schema(schema)
        .json("streams/bridge_temperature")
    )

    vib_raw = (
        spark.readStream
        .schema(schema)
        .json("streams/bridge_vibration")
    )

    tilt_raw = (
        spark.readStream
        .schema(schema)
        .json("streams/bridge_tilt")
    )

    def enrich(df):
        return (
            df.withColumn("event_time_ts", to_timestamp("event_time"))
              .withColumn("ingest_time_ts", to_timestamp("ingest_time"))
              .withColumn("partition_date", to_date(col("event_time_ts")))
        )

    temp_df = enrich(temp_raw)
    vib_df  = enrich(vib_raw)
    tilt_df = enrich(tilt_raw)

    def split_valid_invalid(df):
        valid = df.where(col("event_time_ts").isNotNull() & col("value").isNotNull())
        invalid = df.where(col("event_time_ts").isNull() | col("value").isNull())
        return valid, invalid

    temp_valid, temp_invalid = split_valid_invalid(temp_df)
    vib_valid,  vib_invalid  = split_valid_invalid(vib_df)
    tilt_valid, tilt_invalid = split_valid_invalid(tilt_df)

    # --------------------------------------------------------
    # Write valid records to Bronze layer (Parquet)
    # --------------------------------------------------------
    q_temp = (
        temp_valid.writeStream
        .format("parquet")
        .option("checkpointLocation", "checkpoints/bronze_temperature")
        .option("path", "bronze/bridge_temperature")
        .outputMode("append")
        .start()
    )

    q_vib = (
        vib_valid.writeStream
        .format("parquet")
        .option("checkpointLocation", "checkpoints/bronze_vibration")
        .option("path", "bronze/bridge_vibration")
        .outputMode("append")
        .start()
    )

    q_tilt = (
        tilt_valid.writeStream
        .format("parquet")
        .option("checkpointLocation", "checkpoints/bronze_tilt")
        .option("path", "bronze/bridge_tilt")
        .outputMode("append")
        .start()
    )

    # --------------------------------------------------------
    # Write invalid records to rejected area
    # --------------------------------------------------------
    rejected_all = (
        temp_invalid.unionByName(vib_invalid, allowMissingColumns=True)
                    .unionByName(tilt_invalid, allowMissingColumns=True)
    )

    q_rejected = (
        rejected_all.writeStream
        .format("parquet")
        .option("checkpointLocation", "checkpoints/bronze_rejected")
        .option("path", "bronze/rejected")
        .outputMode("append")
        .start()
    )

    # --------------------------------------------------------
    # EXTRA: console output so you see something on screen
    # --------------------------------------------------------
    temp_valid_tagged = temp_valid.withColumn("source", lit("temperature"))
    vib_valid_tagged  = vib_valid.withColumn("source", lit("vibration"))
    tilt_valid_tagged = tilt_valid.withColumn("source", lit("tilt"))

    all_valid = (
        temp_valid_tagged
        .unionByName(vib_valid_tagged, allowMissingColumns=True)
        .unionByName(tilt_valid_tagged, allowMissingColumns=True)
    )

    q_console = (
        all_valid.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 10)
        .start()
    )

    # --------------------------------------------------------
    # Run for a limited time, then stop everything
    # --------------------------------------------------------
    print(f"Bronze streaming started, will run for {run_seconds} seconds...")
    start = time.time()

    try:
        while time.time() - start < run_seconds:
            time.sleep(5)  # just sleep in small chunks
    finally:
        print("Stopping bronze queries...")
        for q in [q_temp, q_vib, q_tilt, q_rejected, q_console]:
            try:
                q.stop()
            except Exception as e:
                print("Error stopping query:", e)

        spark.stop()
        print("Bronze pipeline finished.")


if __name__ == "__main__":
    # default 30 seconds if run as a script
    main()
