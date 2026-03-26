"""Bronze layer: raw event ingestion to Parquet with partitioning.

The bronze layer stores raw, unprocessed IoT sensor events exactly as
received from Kafka, with minimal transformation (adding ingestion
metadata and partitioning).
"""

from __future__ import annotations

from datetime import datetime

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import get_settings, get_yaml_config

logger = structlog.get_logger(__name__)


class BronzeLayer:
    """Manages the bronze layer of the data lakehouse.

    The bronze layer is the raw data landing zone where events are
    stored with minimal transformation, preserving the original data
    for auditability and reprocessing.

    Attributes:
        spark: Active SparkSession.
        bronze_path: Storage path for bronze layer data.
        partition_columns: Columns used for Parquet partitioning.
    """

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the bronze layer manager.

        Args:
            spark: Active SparkSession instance.
        """
        self.spark = spark
        settings = get_settings()
        yaml_config = get_yaml_config()
        bronze_config = yaml_config.get("lakehouse", {}).get("bronze", {})

        self.bronze_path = settings.lakehouse.bronze_path
        self.partition_columns = bronze_config.get(
            "partition_columns", ["event_date", "sensor_type"]
        )
        self.retention_days = bronze_config.get("retention_days", 90)

        logger.info("bronze_layer_initialized", path=self.bronze_path)

    def ingest_batch(self, df: DataFrame) -> int:
        """Ingest a batch DataFrame into the bronze layer.

        Adds ingestion metadata columns and writes to Parquet
        with date and sensor type partitioning.

        Args:
            df: DataFrame of raw IoT events.

        Returns:
            Number of rows ingested.
        """
        enriched = (
            df.withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("event_date", F.to_date("timestamp"))
            .withColumn("source", F.lit("kafka"))
            .withColumn("pipeline_version", F.lit("1.0.0"))
        )

        row_count = enriched.count()

        enriched.write.mode("append").partitionBy(*self.partition_columns).parquet(self.bronze_path)

        logger.info(
            "bronze_batch_ingested",
            row_count=row_count,
            path=self.bronze_path,
            partitions=self.partition_columns,
        )
        return row_count

    def read(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        sensor_type: str | None = None,
    ) -> DataFrame:
        """Read data from the bronze layer with optional filters.

        Args:
            start_date: Start date filter (YYYY-MM-DD).
            end_date: End date filter (YYYY-MM-DD).
            sensor_type: Filter by sensor type.

        Returns:
            Filtered DataFrame from the bronze layer.
        """
        df = self.spark.read.parquet(self.bronze_path)

        if start_date:
            df = df.filter(F.col("event_date") >= start_date)
        if end_date:
            df = df.filter(F.col("event_date") <= end_date)
        if sensor_type:
            df = df.filter(F.col("sensor_type") == sensor_type)

        logger.info(
            "bronze_data_read",
            start_date=start_date,
            end_date=end_date,
            sensor_type=sensor_type,
        )
        return df

    def get_stats(self) -> dict:
        """Get statistics about the bronze layer.

        Returns:
            Dictionary with row counts, date range, and sensor type distribution.
        """
        try:
            df = self.spark.read.parquet(self.bronze_path)
            stats = {
                "total_rows": df.count(),
                "date_range": {
                    "min": str(df.agg(F.min("event_date")).collect()[0][0]),
                    "max": str(df.agg(F.max("event_date")).collect()[0][0]),
                },
                "sensor_types": {
                    row["sensor_type"]: row["count"]
                    for row in df.groupBy("sensor_type").count().collect()
                },
                "layer": "bronze",
            }
            logger.info("bronze_stats_computed", total_rows=stats["total_rows"])
            return stats
        except Exception as exc:
            logger.warning("bronze_stats_failed", error=str(exc))
            return {"total_rows": 0, "layer": "bronze", "error": str(exc)}

    def cleanup_expired(self) -> int:
        """Remove data older than the retention period.

        Returns:
            Number of partitions cleaned up.
        """
        cutoff = datetime.utcnow().date().isoformat()
        logger.info(
            "bronze_cleanup_requested",
            retention_days=self.retention_days,
            cutoff=cutoff,
        )
        # In production, this would delete old partitions
        # For safety, we log but don't auto-delete
        return 0
