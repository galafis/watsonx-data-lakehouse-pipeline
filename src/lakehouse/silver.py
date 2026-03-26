"""Silver layer: cleaning, deduplication, and enrichment.

The silver layer applies data quality transformations to bronze data,
including deduplication, null handling, type casting, and enrichment
with derived columns.
"""

from __future__ import annotations

from typing import Optional

import structlog
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.config import get_settings, get_yaml_config

logger = structlog.get_logger(__name__)


class SilverLayer:
    """Manages the silver layer of the data lakehouse.

    The silver layer contains cleaned, deduplicated, and enriched
    data ready for analytical processing and KPI calculation.

    Attributes:
        spark: Active SparkSession.
        silver_path: Storage path for silver layer data.
        bronze_path: Source path for bronze layer data.
    """

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the silver layer manager.

        Args:
            spark: Active SparkSession instance.
        """
        self.spark = spark
        settings = get_settings()
        yaml_config = get_yaml_config()
        silver_config = yaml_config.get("lakehouse", {}).get("silver", {})

        self.silver_path = settings.lakehouse.silver_path
        self.bronze_path = settings.lakehouse.bronze_path
        self.partition_columns = silver_config.get(
            "partition_columns", ["event_date", "facility_id"]
        )
        self.dedup_window_hours = silver_config.get("dedup_window_hours", 24)

        logger.info("silver_layer_initialized", path=self.silver_path)

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """Remove duplicate events based on event_id.

        Keeps the first occurrence of each event, ordered by
        ingestion timestamp.

        Args:
            df: DataFrame with potential duplicates.

        Returns:
            Deduplicated DataFrame.
        """
        window = Window.partitionBy("event_id").orderBy(F.col("ingestion_timestamp").asc())

        deduped = (
            df.withColumn("row_num", F.row_number().over(window))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

        original_count = df.count()
        deduped_count = deduped.count()
        duplicates_removed = original_count - deduped_count

        logger.info(
            "deduplication_complete",
            original=original_count,
            deduped=deduped_count,
            removed=duplicates_removed,
        )
        return deduped

    def clean(self, df: DataFrame) -> DataFrame:
        """Apply data cleaning transformations.

        - Fills null quality_score with 0.0
        - Casts value to double
        - Trims string columns
        - Filters out records with invalid sensor types

        Args:
            df: Raw DataFrame from bronze layer.

        Returns:
            Cleaned DataFrame.
        """
        cleaned = (
            df.withColumn("quality_score", F.coalesce(F.col("quality_score"), F.lit(1.0)))
            .withColumn("is_anomaly", F.coalesce(F.col("is_anomaly"), F.lit(False)))
            .withColumn("value", F.col("value").cast("double"))
            .withColumn("sensor_id", F.trim(F.col("sensor_id")))
            .withColumn("facility_id", F.trim(F.col("facility_id")))
            .filter(F.col("value").isNotNull())
            .filter(
                F.col("sensor_type").isin(
                    "temperature", "vibration", "pressure", "throughput"
                )
            )
        )

        logger.info("data_cleaning_complete")
        return cleaned

    def enrich(self, df: DataFrame) -> DataFrame:
        """Enrich data with derived analytical columns.

        Adds rolling statistics, z-scores, and time-based features
        for downstream analytics.

        Args:
            df: Cleaned DataFrame.

        Returns:
            Enriched DataFrame with additional columns.
        """
        # Rolling window for statistics (per sensor, last 100 readings)
        sensor_window = (
            Window.partitionBy("sensor_id", "sensor_type")
            .orderBy("timestamp")
            .rowsBetween(-99, 0)
        )

        enriched = (
            df.withColumn("rolling_mean", F.avg("value").over(sensor_window))
            .withColumn("rolling_stddev", F.stddev("value").over(sensor_window))
            .withColumn(
                "z_score",
                F.when(
                    F.col("rolling_stddev") > 0,
                    (F.col("value") - F.col("rolling_mean")) / F.col("rolling_stddev"),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn("hour_of_day", F.hour("timestamp"))
            .withColumn("day_of_week", F.dayofweek("timestamp"))
            .withColumn(
                "is_business_hours",
                (F.col("hour_of_day").between(8, 17)) & (F.col("day_of_week").between(2, 6)),
            )
            .withColumn("event_date", F.to_date("timestamp"))
        )

        logger.info("data_enrichment_complete")
        return enriched

    def process(self, incremental: bool = True) -> int:
        """Run the full silver layer processing pipeline.

        Args:
            incremental: If True, only process new bronze data.

        Returns:
            Number of rows written to silver.
        """
        bronze_df = self.spark.read.parquet(self.bronze_path)

        if incremental:
            try:
                existing_silver = self.spark.read.parquet(self.silver_path)
                max_timestamp = existing_silver.agg(
                    F.max("ingestion_timestamp")
                ).collect()[0][0]
                if max_timestamp:
                    bronze_df = bronze_df.filter(
                        F.col("ingestion_timestamp") > max_timestamp
                    )
            except Exception:
                logger.info("silver_full_load", reason="no_existing_silver_data")

        deduped = self.deduplicate(bronze_df)
        cleaned = self.clean(deduped)
        enriched = self.enrich(cleaned)

        row_count = enriched.count()
        if row_count > 0:
            enriched.write.mode("append").partitionBy(*self.partition_columns).parquet(
                self.silver_path
            )
            logger.info("silver_data_written", row_count=row_count, path=self.silver_path)

        return row_count

    def read(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        facility_id: Optional[str] = None,
    ) -> DataFrame:
        """Read data from the silver layer with optional filters.

        Args:
            start_date: Start date filter (YYYY-MM-DD).
            end_date: End date filter (YYYY-MM-DD).
            facility_id: Filter by facility ID.

        Returns:
            Filtered DataFrame from the silver layer.
        """
        df = self.spark.read.parquet(self.silver_path)

        if start_date:
            df = df.filter(F.col("event_date") >= start_date)
        if end_date:
            df = df.filter(F.col("event_date") <= end_date)
        if facility_id:
            df = df.filter(F.col("facility_id") == facility_id)

        return df

    def get_stats(self) -> dict:
        """Get statistics about the silver layer.

        Returns:
            Dictionary with row counts and quality metrics.
        """
        try:
            df = self.spark.read.parquet(self.silver_path)
            return {
                "total_rows": df.count(),
                "avg_quality_score": float(
                    df.agg(F.avg("quality_score")).collect()[0][0] or 0
                ),
                "anomaly_rate": float(
                    df.agg(F.avg(F.col("is_anomaly").cast("double"))).collect()[0][0] or 0
                ),
                "layer": "silver",
            }
        except Exception as exc:
            logger.warning("silver_stats_failed", error=str(exc))
            return {"total_rows": 0, "layer": "silver", "error": str(exc)}
