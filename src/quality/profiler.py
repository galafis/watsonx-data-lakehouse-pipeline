"""Automated data profiling and report generation.

Profiles IoT sensor data to compute distribution statistics,
detect patterns, and generate quality reports.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import structlog

from src.config import get_yaml_config

logger = structlog.get_logger(__name__)


@dataclass
class ColumnProfile:
    """Profile statistics for a single column.

    Attributes:
        name: Column name.
        dtype: Column data type.
        null_count: Number of null values.
        null_rate: Percentage of null values.
        distinct_count: Number of distinct values.
        stats: Additional statistics (min, max, mean, etc.).
    """

    name: str
    dtype: str
    null_count: int = 0
    null_rate: float = 0.0
    distinct_count: int = 0
    stats: dict[str, Any] = field(default_factory=dict)


@dataclass
class DataProfile:
    """Complete data profile for a DataFrame.

    Attributes:
        name: Profile name/identifier.
        row_count: Total number of rows.
        column_count: Number of columns.
        columns: Per-column profiles.
        timestamp: When the profile was generated.
    """

    name: str
    row_count: int = 0
    column_count: int = 0
    columns: list[ColumnProfile] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "name": self.name,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "timestamp": self.timestamp.isoformat(),
            "columns": [
                {
                    "name": c.name,
                    "dtype": c.dtype,
                    "null_count": c.null_count,
                    "null_rate": round(c.null_rate, 4),
                    "distinct_count": c.distinct_count,
                    "stats": c.stats,
                }
                for c in self.columns
            ],
        }


class DataProfiler:
    """Automated data profiler for IoT sensor DataFrames.

    Generates comprehensive data profiles including distribution
    statistics, null analysis, and pattern detection.

    Attributes:
        sample_size: Maximum number of rows to profile.
    """

    def __init__(self, sample_size: int | None = None) -> None:
        """Initialize the profiler.

        Args:
            sample_size: Max rows to sample for profiling. If None,
                         uses value from settings.yaml.
        """
        yaml_config = get_yaml_config()
        profiling_config = yaml_config.get("quality", {}).get("profiling", {})
        self.sample_size = sample_size or profiling_config.get("sample_size", 10000)

        logger.info("profiler_initialized", sample_size=self.sample_size)

    def profile(self, df: Any, name: str = "iot_data") -> DataProfile:
        """Generate a complete profile for a DataFrame.

        Args:
            df: PySpark DataFrame to profile.
            name: Name for this profile.

        Returns:
            DataProfile with per-column statistics.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import DoubleType, FloatType, IntegerType, LongType

        total_rows = df.count()
        sampled = df.limit(self.sample_size) if total_rows > self.sample_size else df

        profile = DataProfile(
            name=name,
            row_count=total_rows,
            column_count=len(df.columns),
        )

        numeric_types = (DoubleType, FloatType, IntegerType, LongType)

        for col_field in df.schema.fields:
            col_name = col_field.name
            col_type = col_field.dataType

            null_count = sampled.filter(F.col(col_name).isNull()).count()
            sampled_count = sampled.count()
            null_rate = null_count / sampled_count if sampled_count > 0 else 0

            distinct_count = sampled.select(col_name).distinct().count()

            stats: dict[str, Any] = {}

            if isinstance(col_type, numeric_types):
                agg_result = sampled.agg(
                    F.min(col_name).alias("min"),
                    F.max(col_name).alias("max"),
                    F.avg(col_name).alias("mean"),
                    F.stddev(col_name).alias("stddev"),
                    F.expr(f"percentile_approx({col_name}, 0.25)").alias("p25"),
                    F.expr(f"percentile_approx({col_name}, 0.50)").alias("median"),
                    F.expr(f"percentile_approx({col_name}, 0.75)").alias("p75"),
                ).collect()[0]

                stats = {
                    "min": float(agg_result["min"]) if agg_result["min"] is not None else None,
                    "max": float(agg_result["max"]) if agg_result["max"] is not None else None,
                    "mean": round(float(agg_result["mean"]), 4) if agg_result["mean"] else None,
                    "stddev": round(float(agg_result["stddev"]), 4) if agg_result["stddev"] else None,
                    "p25": float(agg_result["p25"]) if agg_result["p25"] is not None else None,
                    "median": float(agg_result["median"]) if agg_result["median"] is not None else None,
                    "p75": float(agg_result["p75"]) if agg_result["p75"] is not None else None,
                }

            column_profile = ColumnProfile(
                name=col_name,
                dtype=str(col_type),
                null_count=null_count,
                null_rate=null_rate,
                distinct_count=distinct_count,
                stats=stats,
            )
            profile.columns.append(column_profile)

        logger.info(
            "profiling_complete",
            name=name,
            rows=total_rows,
            columns=len(profile.columns),
        )
        return profile

    def generate_report(self, profile: DataProfile) -> dict[str, Any]:
        """Generate a summary report from a data profile.

        Args:
            profile: DataProfile to summarize.

        Returns:
            Dictionary with report summary and recommendations.
        """
        high_null_columns = [
            c.name for c in profile.columns if c.null_rate > 0.05
        ]
        low_cardinality = [
            c.name for c in profile.columns if c.distinct_count <= 5
        ]

        recommendations = []
        if high_null_columns:
            recommendations.append(
                f"Columns with high null rates (>5%): {', '.join(high_null_columns)}"
            )
        if low_cardinality:
            recommendations.append(
                f"Low cardinality columns (consider as categoricals): {', '.join(low_cardinality)}"
            )

        report = {
            "profile": profile.to_dict(),
            "summary": {
                "total_rows": profile.row_count,
                "total_columns": profile.column_count,
                "high_null_columns": high_null_columns,
                "low_cardinality_columns": low_cardinality,
            },
            "recommendations": recommendations,
        }

        logger.info("report_generated", name=profile.name)
        return report
