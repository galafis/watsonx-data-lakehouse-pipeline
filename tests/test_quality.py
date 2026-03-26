"""Tests for data quality expectations and automated profiling.

Tests cover:
- ExpectationResult and QualitySuiteResult data classes
- IoTDataQualitySuite schema, null, range, and statistical checks
- DataProfiler column profiling and report generation
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.quality.expectations import (
    ExpectationResult,
    IoTDataQualitySuite,
    QualitySuiteResult,
)
from src.quality.profiler import ColumnProfile, DataProfile, DataProfiler


class TestExpectationResult:
    """Tests for the ExpectationResult data class."""

    def test_creation_with_defaults(self) -> None:
        """Verify ExpectationResult uses correct default values."""
        result = ExpectationResult(name="test_check", success=True)
        assert result.name == "test_check"
        assert result.success is True
        assert result.observed_value is None
        assert result.details == ""

    def test_creation_with_all_fields(self) -> None:
        """Verify ExpectationResult stores all provided fields."""
        result = ExpectationResult(
            name="null_check",
            success=False,
            observed_value=0.15,
            details="Null rate exceeded threshold",
        )
        assert result.observed_value == 0.15
        assert "threshold" in result.details


class TestQualitySuiteResult:
    """Tests for the QualitySuiteResult data class."""

    def test_overall_success_all_pass(self) -> None:
        """Verify overall_success is True when all checks pass."""
        suite = QualitySuiteResult(suite_name="test")
        suite.results = [
            ExpectationResult(name="a", success=True),
            ExpectationResult(name="b", success=True),
        ]
        assert suite.overall_success is True

    def test_overall_success_one_fails(self) -> None:
        """Verify overall_success is False when any check fails."""
        suite = QualitySuiteResult(suite_name="test")
        suite.results = [
            ExpectationResult(name="a", success=True),
            ExpectationResult(name="b", success=False),
        ]
        assert suite.overall_success is False

    def test_success_rate_calculation(self) -> None:
        """Verify success_rate is computed correctly."""
        suite = QualitySuiteResult(suite_name="test")
        suite.results = [
            ExpectationResult(name="a", success=True),
            ExpectationResult(name="b", success=True),
            ExpectationResult(name="c", success=False),
        ]
        assert abs(suite.success_rate - 2 / 3) < 0.001

    def test_success_rate_empty_results(self) -> None:
        """Verify success_rate is 1.0 for empty results."""
        suite = QualitySuiteResult(suite_name="test")
        assert suite.success_rate == 1.0

    def test_to_dict_structure(self) -> None:
        """Verify to_dict returns expected keys."""
        suite = QualitySuiteResult(suite_name="test")
        suite.results = [ExpectationResult(name="a", success=True)]
        d = suite.to_dict()

        assert "suite_name" in d
        assert "run_timestamp" in d
        assert "overall_success" in d
        assert "success_rate" in d
        assert "total_checks" in d
        assert d["passed"] == 1
        assert d["failed"] == 0


class TestIoTDataQualitySuite:
    """Tests for the IoTDataQualitySuite class."""

    @patch("src.quality.expectations.get_yaml_config")
    def test_initialization_loads_config(self, mock_yaml: MagicMock) -> None:
        """Verify suite loads expectation config from YAML."""
        mock_yaml.return_value = {
            "quality": {
                "expectations": {
                    "temperature": {"min_value": -50, "max_value": 500, "null_threshold": 0.01},
                }
            }
        }
        suite = IoTDataQualitySuite()
        assert "temperature" in suite.config

    @patch("src.quality.expectations.get_yaml_config")
    def test_check_schema_all_columns_present(self, mock_yaml: MagicMock) -> None:
        """Verify check_schema passes when all required columns exist."""
        mock_yaml.return_value = {"quality": {"expectations": {}}}
        suite = IoTDataQualitySuite()

        mock_df = MagicMock()
        mock_df.columns = [
            "event_id",
            "sensor_id",
            "sensor_type",
            "facility_id",
            "timestamp",
            "value",
            "unit",
        ]

        results = suite.check_schema(mock_df)

        assert len(results) == 7
        assert all(r.success for r in results)

    @patch("src.quality.expectations.get_yaml_config")
    def test_check_schema_missing_column(self, mock_yaml: MagicMock) -> None:
        """Verify check_schema fails when a required column is missing."""
        mock_yaml.return_value = {"quality": {"expectations": {}}}
        suite = IoTDataQualitySuite()

        mock_df = MagicMock()
        mock_df.columns = ["event_id", "sensor_id"]

        results = suite.check_schema(mock_df)

        failed = [r for r in results if not r.success]
        assert len(failed) == 5  # 7 required - 2 present = 5 missing

    @patch("src.quality.expectations.get_yaml_config")
    def test_check_nulls_empty_dataset(self, mock_yaml: MagicMock) -> None:
        """Verify check_nulls reports failure for empty dataset."""
        mock_yaml.return_value = {
            "quality": {"expectations": {"temperature": {"null_threshold": 0.01}}}
        }
        suite = IoTDataQualitySuite()

        mock_df = MagicMock()
        mock_df.count.return_value = 0

        results = suite.check_nulls(mock_df)

        assert len(results) == 1
        assert results[0].name == "non_empty_dataset"
        assert results[0].success is False

    @patch("src.quality.expectations.get_yaml_config")
    def test_run_suite_aggregates_all_checks(self, mock_yaml: MagicMock) -> None:
        """Verify run_suite combines schema, null, range, and statistical checks."""
        mock_yaml.return_value = {"quality": {"expectations": {}}}
        suite = IoTDataQualitySuite()

        mock_df = MagicMock()
        mock_df.columns = [
            "event_id",
            "sensor_id",
            "sensor_type",
            "facility_id",
            "timestamp",
            "value",
            "unit",
        ]

        with patch.object(suite, "check_nulls", return_value=[]):
            with patch.object(suite, "check_ranges", return_value=[]):
                with patch.object(suite, "check_statistical", return_value=[]):
                    result = suite.run_suite(mock_df, suite_name="test_suite")

        assert result.suite_name == "test_suite"
        assert isinstance(result, QualitySuiteResult)
        assert len(result.results) == 7  # Only schema checks


class TestColumnProfile:
    """Tests for the ColumnProfile data class."""

    def test_creation_with_defaults(self) -> None:
        """Verify ColumnProfile uses correct default values."""
        profile = ColumnProfile(name="value", dtype="DoubleType")
        assert profile.null_count == 0
        assert profile.null_rate == 0.0
        assert profile.distinct_count == 0
        assert profile.stats == {}


class TestDataProfile:
    """Tests for the DataProfile data class."""

    def test_to_dict_structure(self) -> None:
        """Verify to_dict returns expected structure."""
        profile = DataProfile(name="test", row_count=100, column_count=5)
        profile.columns.append(
            ColumnProfile(name="col1", dtype="StringType", null_count=5, null_rate=0.05)
        )
        d = profile.to_dict()

        assert d["name"] == "test"
        assert d["row_count"] == 100
        assert d["column_count"] == 5
        assert len(d["columns"]) == 1
        assert d["columns"][0]["null_rate"] == 0.05


class TestDataProfiler:
    """Tests for the DataProfiler class."""

    @patch("src.quality.profiler.get_yaml_config")
    def test_initialization(self, mock_yaml: MagicMock) -> None:
        """Verify profiler initializes with sample_size from config."""
        mock_yaml.return_value = {"quality": {"profiling": {"sample_size": 5000}}}
        profiler = DataProfiler()
        assert profiler.sample_size == 5000

    @patch("src.quality.profiler.get_yaml_config")
    def test_initialization_custom_sample_size(self, mock_yaml: MagicMock) -> None:
        """Verify profiler uses custom sample_size when provided."""
        mock_yaml.return_value = {"quality": {"profiling": {}}}
        profiler = DataProfiler(sample_size=200)
        assert profiler.sample_size == 200

    @patch("src.quality.profiler.get_yaml_config")
    def test_generate_report_detects_high_nulls(self, mock_yaml: MagicMock) -> None:
        """Verify report flags columns with high null rates."""
        mock_yaml.return_value = {"quality": {"profiling": {}}}
        profiler = DataProfiler()

        profile = DataProfile(name="test", row_count=1000, column_count=3)
        profile.columns = [
            ColumnProfile(name="good_col", dtype="StringType", null_rate=0.01),
            ColumnProfile(name="bad_col", dtype="StringType", null_rate=0.15),
        ]

        report = profiler.generate_report(profile)

        assert "bad_col" in report["summary"]["high_null_columns"]
        assert "good_col" not in report["summary"]["high_null_columns"]

    @patch("src.quality.profiler.get_yaml_config")
    def test_generate_report_detects_low_cardinality(self, mock_yaml: MagicMock) -> None:
        """Verify report flags columns with low cardinality."""
        mock_yaml.return_value = {"quality": {"profiling": {}}}
        profiler = DataProfiler()

        profile = DataProfile(name="test", row_count=1000, column_count=2)
        profile.columns = [
            ColumnProfile(name="sensor_type", dtype="StringType", distinct_count=4),
            ColumnProfile(name="event_id", dtype="StringType", distinct_count=1000),
        ]

        report = profiler.generate_report(profile)

        assert "sensor_type" in report["summary"]["low_cardinality_columns"]
        assert "event_id" not in report["summary"]["low_cardinality_columns"]

    @patch("src.quality.profiler.get_yaml_config")
    def test_generate_report_no_recommendations(self, mock_yaml: MagicMock) -> None:
        """Verify report has no recommendations for clean data."""
        mock_yaml.return_value = {"quality": {"profiling": {}}}
        profiler = DataProfiler()

        profile = DataProfile(name="test", row_count=1000, column_count=1)
        profile.columns = [
            ColumnProfile(name="value", dtype="DoubleType", null_rate=0.0, distinct_count=500),
        ]

        report = profiler.generate_report(profile)

        assert len(report["recommendations"]) == 0
