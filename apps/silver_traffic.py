from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Spark Session Config
spark = (
    SparkSession.builder
    .appName("traffic-silver-layer")
    .master("spark://spark-master:7077")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Read Bronze Stream
bronze_df = (
    spark.readStream
    .format("delta")
    .load("/opt/spark/warehouse/bronze_traffic")
)


# Data Quality 
dirty_df = bronze_df.filter(
    F.col("vehicle_id").isNotNull()
    & (F.length(F.trim(F.col("vehicle_id"))) > 0)
    & F.col("event_time").isNotNull()
    & (
        ~F.coalesce(
            F.col("raw_json").contains("CORRUPTED"),
            F.lit(False)
        )
    )
)


# Safe Type Casting
dirty_df = (
    dirty_df
    .withColumn(
        "speed",
        F.expr("try_cast(speed AS INT)")
    )
    .withColumn(
        "event_time",
        F.try_to_timestamp("event_time")
    )
)


# Business Validation Rules
dirty_df = dirty_df.filter(
    F.col("speed").between(0, 100)
    & (
        F.col("event_time")
        <= F.current_timestamp() + F.expr("INTERVAL 10 MINUTES")
    )
)


# Handle Late Data
watermarked_df = dirty_df.withWatermark(
    "event_time",
    "15 minutes"
)


# Deduplication
cleaned_df = watermarked_df.dropDuplicates(
    ["vehicle_id", "event_time"]
)


# Feature Engineering
silver_df = (
    cleaned_df
    .withColumn(
        "hour",
        F.hour("event_time")
    )
    .withColumn(
        "peak_flag",
        F.when(
            F.col("hour").between(7, 9)
            | F.col("hour").between(15, 18),
            1
        ).otherwise(0)
    )
    .withColumn(
        "speed_category",
        F.when(F.col("speed") < 30, "LOW")
        .when(F.col("speed") < 70, "MEDIUM")
        .otherwise("HIGH")
    )
)


# Write Silver Table
silver_query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/spark/warehouse/chk/silver_traffic"
    )
    .option(
        "path",
        "/opt/spark/warehouse/silver_traffic"
    )
    .start()
)

silver_query.awaitTermination()