"""Spark Structured Streaming consumer for IoT sensor events.

Reads events from Kafka, validates against the IoT schema,
and writes to the bronze layer of the data lakehouse.
"""

from __future__ import annotations

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import get_settings, get_yaml_config
from src.ingestion.schema_registry import get_spark_schema

logger = structlog.get_logger(__name__)


def create_spark_session() -> SparkSession:
    """Create a configured SparkSession with Iceberg and S3 support.

    Returns:
        Configured SparkSession instance.
    """
    settings = get_settings()
    yaml_config = get_yaml_config()
    spark_config = yaml_config.get("spark", {}).get("config", {})
    packages = yaml_config.get("spark", {}).get("packages", [])

    builder = (
        SparkSession.builder.appName(settings.spark.app_name)
        .master(settings.spark.master)
    )

    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    for key, value in spark_config.items():
        builder = builder.config(key, value)

    # Override S3 settings from environment
    builder = (
        builder.config("spark.hadoop.fs.s3a.endpoint", f"http://{settings.minio.endpoint}")
        .config("spark.hadoop.fs.s3a.access.key", settings.minio.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    spark = builder.getOrCreate()
    logger.info("spark_session_created", master=settings.spark.master, app=settings.spark.app_name)
    return spark


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Read streaming data from Kafka IoT topic.

    Args:
        spark: Active SparkSession.

    Returns:
        Streaming DataFrame with parsed IoT events.
    """
    settings = get_settings()
    schema = get_spark_schema()

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", settings.kafka.topic_iot)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        raw_stream.select(
            F.from_json(F.col("value").cast("string"), schema).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .select("data.*", "kafka_timestamp", "kafka_partition", "kafka_offset")
        .withColumn("event_date", F.to_date("timestamp"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    logger.info("kafka_stream_configured", topic=settings.kafka.topic_iot)
    return parsed_stream


def validate_stream(df: DataFrame) -> DataFrame:
    """Apply schema validation filters to the streaming DataFrame.

    Args:
        df: Raw streaming DataFrame.

    Returns:
        Validated DataFrame with invalid records filtered out.
    """
    validated = (
        df.filter(F.col("event_id").isNotNull())
        .filter(F.col("sensor_id").isNotNull())
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("value").isNotNull())
        .filter(
            F.col("sensor_type").isin(
                "temperature", "vibration", "pressure", "throughput"
            )
        )
    )

    logger.info("stream_validation_configured")
    return validated


def write_to_bronze(df: DataFrame, checkpoint_dir: str, output_path: str) -> None:
    """Write validated streaming data to the bronze layer.

    Args:
        df: Validated streaming DataFrame.
        checkpoint_dir: Path for Spark streaming checkpoints.
        output_path: Bronze layer output path.
    """
    query = (
        df.writeStream.format("parquet")
        .option("checkpointLocation", checkpoint_dir)
        .option("path", output_path)
        .partitionBy("event_date", "sensor_type")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info(
        "bronze_stream_started",
        checkpoint=checkpoint_dir,
        output=output_path,
    )
    query.awaitTermination()


def main() -> None:
    """Entry point for the Spark streaming consumer."""
    settings = get_settings()
    yaml_config = get_yaml_config()
    lakehouse_config = yaml_config.get("lakehouse", {}).get("bronze", {})

    spark = create_spark_session()
    stream = read_kafka_stream(spark)
    validated = validate_stream(stream)

    write_to_bronze(
        df=validated,
        checkpoint_dir=lakehouse_config.get(
            "checkpoint_dir", "s3a://lakehouse/checkpoints/bronze"
        ),
        output_path=settings.lakehouse.bronze_path,
    )


if __name__ == "__main__":
    main()
