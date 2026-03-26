"""Gold layer: KPI aggregation and anomaly scoring.

The gold layer contains business-level aggregations, KPI metrics,
and anomaly scores ready for consumption by dashboards and reports.
"""

from __future__ import annotations

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import get_settings, get_yaml_config

logger = structlog.get_logger(__name__)


class GoldLayer:
    """Manages the gold layer of the data lakehouse.

    The gold layer produces aggregated KPIs and anomaly scores
    for operational dashboards and analytics.

    Attributes:
        spark: Active SparkSession.
        gold_path: Storage path for gold layer data.
        silver_path: Source path for silver layer data.
    """

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the gold layer manager.

        Args:
            spark: Active SparkSession instance.
        """
        self.spark = spark
        settings = get_settings()
        yaml_config = get_yaml_config()
        gold_config = yaml_config.get("lakehouse", {}).get("gold", {})

        self.gold_path = settings.lakehouse.gold_path
        self.silver_path = settings.lakehouse.silver_path
        self.partition_columns = gold_config.get("partition_columns", ["kpi_date", "facility_id"])
        self.aggregation_window = gold_config.get("aggregation_window_minutes", 15)

        logger.info("gold_layer_initialized", path=self.gold_path)

    def aggregate_kpis(self, df: DataFrame) -> DataFrame:
        """Aggregate sensor data into operational KPIs.

        Computes per-facility, per-sensor-type KPIs including:
        - Mean, min, max, stddev of sensor values
        - Anomaly count and rate
        - Data quality score average
        - Event count

        Args:
            df: Silver layer DataFrame.

        Returns:
            Aggregated KPI DataFrame.
        """
        kpis = (
            df.withColumn(
                "time_window",
                F.window("timestamp", f"{self.aggregation_window} minutes"),
            )
            .groupBy("facility_id", "sensor_type", "time_window")
            .agg(
                F.count("*").alias("event_count"),
                F.avg("value").alias("avg_value"),
                F.min("value").alias("min_value"),
                F.max("value").alias("max_value"),
                F.stddev("value").alias("stddev_value"),
                F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
                F.avg(F.col("is_anomaly").cast("double")).alias("anomaly_rate"),
                F.avg("quality_score").alias("avg_quality_score"),
                F.avg("z_score").alias("avg_z_score"),
                F.max(F.abs("z_score")).alias("max_abs_z_score"),
            )
            .withColumn("window_start", F.col("time_window.start"))
            .withColumn("window_end", F.col("time_window.end"))
            .withColumn("kpi_date", F.to_date("window_start"))
            .withColumn("computed_at", F.current_timestamp())
            .drop("time_window")
        )

        logger.info("kpi_aggregation_complete", window_minutes=self.aggregation_window)
        return kpis

    def compute_anomaly_scores(self, df: DataFrame) -> DataFrame:
        """Compute composite anomaly scores per facility.

        Combines z-score magnitude, anomaly rate, and quality
        degradation into a single anomaly severity score.

        Args:
            df: Silver layer DataFrame.

        Returns:
            DataFrame with facility-level anomaly scores.
        """
        anomaly_scores = (
            df.groupBy("facility_id", F.to_date("timestamp").alias("score_date"))
            .agg(
                F.avg(F.abs("z_score")).alias("avg_abs_z_score"),
                F.avg(F.col("is_anomaly").cast("double")).alias("anomaly_rate"),
                F.avg("quality_score").alias("avg_quality"),
                F.count("*").alias("total_events"),
                F.sum(F.col("is_anomaly").cast("int")).alias("total_anomalies"),
            )
            .withColumn(
                "anomaly_severity",
                (
                    F.col("avg_abs_z_score") * 0.4
                    + F.col("anomaly_rate") * 100 * 0.4
                    + (1 - F.col("avg_quality")) * 100 * 0.2
                ),
            )
            .withColumn(
                "severity_level",
                F.when(F.col("anomaly_severity") > 50, "CRITICAL")
                .when(F.col("anomaly_severity") > 25, "WARNING")
                .when(F.col("anomaly_severity") > 10, "ELEVATED")
                .otherwise("NORMAL"),
            )
            .withColumn("computed_at", F.current_timestamp())
        )

        logger.info("anomaly_scores_computed")
        return anomaly_scores

    def process(self) -> int:
        """Run the full gold layer processing pipeline.

        Returns:
            Number of KPI rows written.
        """
        silver_df = self.spark.read.parquet(self.silver_path)

        kpis = self.aggregate_kpis(silver_df)
        anomaly_scores = self.compute_anomaly_scores(silver_df)

        kpi_count = kpis.count()
        kpis.write.mode("overwrite").partitionBy(*self.partition_columns).parquet(
            f"{self.gold_path}/kpis"
        )

        anomaly_scores.write.mode("overwrite").partitionBy("facility_id").parquet(
            f"{self.gold_path}/anomaly_scores"
        )

        logger.info("gold_data_written", kpi_rows=kpi_count, path=self.gold_path)
        return kpi_count

    def read_kpis(
        self,
        facility_id: str | None = None,
        kpi_date: str | None = None,
    ) -> DataFrame:
        """Read KPI data from the gold layer.

        Args:
            facility_id: Filter by facility ID.
            kpi_date: Filter by KPI date (YYYY-MM-DD).

        Returns:
            Filtered KPI DataFrame.
        """
        df = self.spark.read.parquet(f"{self.gold_path}/kpis")

        if facility_id:
            df = df.filter(F.col("facility_id") == facility_id)
        if kpi_date:
            df = df.filter(F.col("kpi_date") == kpi_date)

        return df

    def read_anomaly_scores(self, facility_id: str | None = None) -> DataFrame:
        """Read anomaly scores from the gold layer.

        Args:
            facility_id: Filter by facility ID.

        Returns:
            Filtered anomaly scores DataFrame.
        """
        df = self.spark.read.parquet(f"{self.gold_path}/anomaly_scores")

        if facility_id:
            df = df.filter(F.col("facility_id") == facility_id)

        return df

    def get_stats(self) -> dict:
        """Get statistics about the gold layer.

        Returns:
            Dictionary with KPI counts and anomaly summary.
        """
        try:
            kpis = self.spark.read.parquet(f"{self.gold_path}/kpis")
            anomalies = self.spark.read.parquet(f"{self.gold_path}/anomaly_scores")
            return {
                "total_kpi_rows": kpis.count(),
                "total_anomaly_rows": anomalies.count(),
                "facilities": [
                    row["facility_id"] for row in kpis.select("facility_id").distinct().collect()
                ],
                "layer": "gold",
            }
        except Exception as exc:
            logger.warning("gold_stats_failed", error=str(exc))
            return {"total_kpi_rows": 0, "layer": "gold", "error": str(exc)}
