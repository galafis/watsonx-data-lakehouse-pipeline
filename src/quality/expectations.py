"""Great Expectations data quality suite.

Defines and runs data quality checks including schema validation,
null checks, range checks, and statistical checks for IoT sensor data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import structlog

from src.config import get_yaml_config

logger = structlog.get_logger(__name__)


@dataclass
class ExpectationResult:
    """Result of a single data quality expectation check.

    Attributes:
        name: Expectation name.
        success: Whether the check passed.
        observed_value: The actual value observed.
        details: Additional details about the check.
    """

    name: str
    success: bool
    observed_value: Any = None
    details: str = ""


@dataclass
class QualitySuiteResult:
    """Result of a complete quality suite run.

    Attributes:
        suite_name: Name of the quality suite.
        run_timestamp: When the suite was executed.
        results: List of individual expectation results.
        overall_success: Whether all checks passed.
    """

    suite_name: str
    run_timestamp: datetime = field(default_factory=datetime.utcnow)
    results: list[ExpectationResult] = field(default_factory=list)

    @property
    def overall_success(self) -> bool:
        """Whether all expectations passed."""
        return all(r.success for r in self.results)

    @property
    def success_rate(self) -> float:
        """Percentage of expectations that passed."""
        if not self.results:
            return 1.0
        return sum(1 for r in self.results if r.success) / len(self.results)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "suite_name": self.suite_name,
            "run_timestamp": self.run_timestamp.isoformat(),
            "overall_success": self.overall_success,
            "success_rate": self.success_rate,
            "total_checks": len(self.results),
            "passed": sum(1 for r in self.results if r.success),
            "failed": sum(1 for r in self.results if not r.success),
            "results": [
                {
                    "name": r.name,
                    "success": r.success,
                    "observed_value": r.observed_value,
                    "details": r.details,
                }
                for r in self.results
            ],
        }


class IoTDataQualitySuite:
    """Data quality suite for IoT sensor event data.

    Implements schema validation, null checks, range checks,
    and statistical checks against configurable thresholds.

    Attributes:
        config: Quality expectation thresholds from settings.yaml.
    """

    def __init__(self) -> None:
        """Initialize the quality suite with configuration."""
        yaml_config = get_yaml_config()
        self.config = yaml_config.get("quality", {}).get("expectations", {})
        logger.info("quality_suite_initialized", sensor_types=list(self.config.keys()))

    def check_schema(self, df: Any) -> list[ExpectationResult]:
        """Validate that required columns exist in the DataFrame.

        Args:
            df: PySpark DataFrame to validate.

        Returns:
            List of expectation results for schema checks.
        """
        required_columns = [
            "event_id", "sensor_id", "sensor_type", "facility_id",
            "timestamp", "value", "unit",
        ]
        results = []
        actual_columns = set(df.columns)

        for col in required_columns:
            exists = col in actual_columns
            results.append(
                ExpectationResult(
                    name=f"column_exists_{col}",
                    success=exists,
                    observed_value=exists,
                    details=f"Column '{col}' {'exists' if exists else 'is missing'}",
                )
            )

        logger.info("schema_check_complete", passed=all(r.success for r in results))
        return results

    def check_nulls(self, df: Any) -> list[ExpectationResult]:
        """Check null rates against configured thresholds.

        Args:
            df: PySpark DataFrame to validate.

        Returns:
            List of expectation results for null checks.
        """
        from pyspark.sql import functions as F

        results = []
        total_count = df.count()

        if total_count == 0:
            results.append(
                ExpectationResult(
                    name="non_empty_dataset",
                    success=False,
                    observed_value=0,
                    details="Dataset is empty",
                )
            )
            return results

        for sensor_type, thresholds in self.config.items():
            null_threshold = thresholds.get("null_threshold", 0.05)
            sensor_df = df.filter(F.col("sensor_type") == sensor_type)
            sensor_count = sensor_df.count()

            if sensor_count == 0:
                continue

            null_count = sensor_df.filter(F.col("value").isNull()).count()
            null_rate = null_count / sensor_count

            results.append(
                ExpectationResult(
                    name=f"null_check_{sensor_type}",
                    success=null_rate <= null_threshold,
                    observed_value=round(null_rate, 4),
                    details=(
                        f"{sensor_type}: null rate {null_rate:.2%} "
                        f"(threshold: {null_threshold:.2%})"
                    ),
                )
            )

        logger.info("null_check_complete", checks=len(results))
        return results

    def check_ranges(self, df: Any) -> list[ExpectationResult]:
        """Check that sensor values fall within expected ranges.

        Args:
            df: PySpark DataFrame to validate.

        Returns:
            List of expectation results for range checks.
        """
        from pyspark.sql import functions as F

        results = []

        for sensor_type, thresholds in self.config.items():
            min_val = thresholds.get("min_value")
            max_val = thresholds.get("max_value")

            if min_val is None or max_val is None:
                continue

            sensor_df = df.filter(F.col("sensor_type") == sensor_type)
            sensor_count = sensor_df.count()

            if sensor_count == 0:
                continue

            out_of_range = sensor_df.filter(
                (F.col("value") < min_val) | (F.col("value") > max_val)
            ).count()

            in_range_rate = 1 - (out_of_range / sensor_count)

            results.append(
                ExpectationResult(
                    name=f"range_check_{sensor_type}",
                    success=in_range_rate >= 0.95,
                    observed_value=round(in_range_rate, 4),
                    details=(
                        f"{sensor_type}: {in_range_rate:.2%} within [{min_val}, {max_val}], "
                        f"{out_of_range} out of range"
                    ),
                )
            )

        logger.info("range_check_complete", checks=len(results))
        return results

    def check_statistical(self, df: Any) -> list[ExpectationResult]:
        """Run statistical checks on sensor data distributions.

        Checks for unusual standard deviation and coefficient of variation.

        Args:
            df: PySpark DataFrame to validate.

        Returns:
            List of expectation results for statistical checks.
        """
        from pyspark.sql import functions as F

        results = []

        for sensor_type in self.config:
            sensor_df = df.filter(F.col("sensor_type") == sensor_type)
            stats_row = sensor_df.agg(
                F.avg("value").alias("mean"),
                F.stddev("value").alias("stddev"),
                F.count("value").alias("count"),
            ).collect()

            if not stats_row or stats_row[0]["count"] == 0:
                continue

            mean_val = stats_row[0]["mean"]
            stddev_val = stats_row[0]["stddev"] or 0
            count_val = stats_row[0]["count"]

            # Coefficient of variation should be reasonable (< 100%)
            cv = (stddev_val / mean_val * 100) if mean_val != 0 else 0

            results.append(
                ExpectationResult(
                    name=f"statistical_check_{sensor_type}",
                    success=cv < 100,
                    observed_value=round(cv, 2),
                    details=(
                        f"{sensor_type}: CV={cv:.1f}%, mean={mean_val:.2f}, "
                        f"stddev={stddev_val:.2f}, n={count_val}"
                    ),
                )
            )

        logger.info("statistical_check_complete", checks=len(results))
        return results

    def run_suite(self, df: Any, suite_name: str = "iot_quality_suite") -> QualitySuiteResult:
        """Run the complete data quality suite.

        Args:
            df: PySpark DataFrame to validate.
            suite_name: Name for this suite run.

        Returns:
            QualitySuiteResult with all check results.
        """
        result = QualitySuiteResult(suite_name=suite_name)

        result.results.extend(self.check_schema(df))
        result.results.extend(self.check_nulls(df))
        result.results.extend(self.check_ranges(df))
        result.results.extend(self.check_statistical(df))

        logger.info(
            "quality_suite_complete",
            suite=suite_name,
            overall_success=result.overall_success,
            success_rate=f"{result.success_rate:.2%}",
            total_checks=len(result.results),
        )
        return result
