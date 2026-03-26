"""Shared test fixtures for the lakehouse pipeline tests.

Provides mock SparkSession, sample DataFrames, and common test
utilities used across all test modules.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _mock_pyspark_functions():
    """Patch pyspark.sql.functions to avoid SparkContext requirement in tests."""
    mock_fn = MagicMock()
    patchers = [
        patch("pyspark.sql.functions.col", side_effect=lambda x: MagicMock(name=f"col_{x}")),
        patch("pyspark.sql.functions.lit", side_effect=lambda x: MagicMock(name=f"lit_{x}")),
        patch("pyspark.sql.functions.current_timestamp", return_value=mock_fn),
        patch("pyspark.sql.functions.to_date", side_effect=lambda *a: mock_fn),
        patch("pyspark.sql.functions.when", side_effect=lambda *a: mock_fn),
        patch("pyspark.sql.functions.avg", side_effect=lambda *a: mock_fn),
        patch("pyspark.sql.functions.stddev", side_effect=lambda *a: mock_fn),
        patch("pyspark.sql.functions.min", side_effect=lambda *a: mock_fn),
        patch("pyspark.sql.functions.max", side_effect=lambda *a: mock_fn),
        patch("pyspark.sql.functions.count", side_effect=lambda *a: mock_fn),
        patch("pyspark.sql.functions.sum", side_effect=lambda *a: mock_fn),
        patch("pyspark.sql.functions.window", side_effect=lambda *a, **kw: mock_fn),
    ]
    for p in patchers:
        p.start()
    yield
    for p in patchers:
        p.stop()


@pytest.fixture(autouse=True)
def mock_yaml_config():
    """Mock YAML configuration for all tests."""
    config = {
        "spark": {
            "app_name": "test-pipeline",
            "master": "local[*]",
            "packages": [],
            "config": {},
        },
        "kafka": {
            "bootstrap_servers": "localhost:9092",
            "topics": {"iot_events": "iot-sensor-events"},
        },
        "lakehouse": {
            "bronze": {
                "path": "/tmp/test/bronze",
                "partition_columns": ["event_date", "sensor_type"],
                "retention_days": 90,
                "checkpoint_dir": "/tmp/test/checkpoints/bronze",
            },
            "silver": {
                "path": "/tmp/test/silver",
                "partition_columns": ["event_date", "facility_id"],
                "dedup_window_hours": 24,
            },
            "gold": {
                "path": "/tmp/test/gold",
                "partition_columns": ["kpi_date", "facility_id"],
                "aggregation_window_minutes": 15,
            },
        },
        "quality": {
            "expectations": {
                "temperature": {
                    "min_value": -50.0,
                    "max_value": 500.0,
                    "null_threshold": 0.01,
                },
                "vibration": {
                    "min_value": 0.0,
                    "max_value": 100.0,
                    "null_threshold": 0.02,
                },
                "pressure": {
                    "min_value": 0.0,
                    "max_value": 1000.0,
                    "null_threshold": 0.01,
                },
                "throughput": {
                    "min_value": 0.0,
                    "max_value": 10000.0,
                    "null_threshold": 0.05,
                },
            },
            "profiling": {
                "sample_size": 100,
            },
        },
        "governance": {
            "lineage": {"tracking_enabled": True},
            "sla": {
                "freshness_threshold_minutes": 30,
                "completeness_threshold_percent": 95.0,
                "alert_channels": ["log"],
            },
        },
    }
    with patch("src.config.load_yaml_config", return_value=config):
        with patch("src.config.get_yaml_config", return_value=config):
            yield config


@pytest.fixture()
def mock_settings():
    """Mock application settings."""
    with patch("src.config.get_settings") as mock:
        settings = MagicMock()
        settings.kafka.bootstrap_servers = "localhost:9092"
        settings.kafka.topic_iot = "iot-sensor-events"
        settings.kafka.consumer_group = "lakehouse-pipeline"
        settings.minio.endpoint = "localhost:9000"
        settings.minio.access_key = "minioadmin"
        settings.minio.secret_key = "minioadmin"
        settings.spark.master = "local[*]"
        settings.spark.app_name = "test-pipeline"
        settings.lakehouse.bronze_path = "/tmp/test/bronze"
        settings.lakehouse.silver_path = "/tmp/test/silver"
        settings.lakehouse.gold_path = "/tmp/test/gold"
        mock.return_value = settings
        yield settings


@pytest.fixture()
def mock_spark():
    """Create a mock SparkSession."""
    spark = MagicMock()
    spark.read.parquet.return_value = MagicMock()
    spark.readStream.format.return_value = MagicMock()
    spark.sql.return_value = MagicMock()
    return spark


@pytest.fixture()
def sample_event() -> dict:
    """Return a sample IoT sensor event dictionary."""
    return {
        "event_id": "evt-001",
        "sensor_id": "sensor-temp-001",
        "sensor_type": "temperature",
        "facility_id": "PLANT-A",
        "timestamp": datetime.utcnow().isoformat(),
        "value": 75.5,
        "unit": "celsius",
        "quality_score": 0.98,
        "is_anomaly": False,
        "metadata": {"firmware": "v2.1"},
    }


@pytest.fixture()
def sample_events() -> list[dict]:
    """Return a batch of sample IoT sensor events."""
    base_time = datetime.utcnow()
    events = []
    sensor_types = [
        ("temperature", "celsius", 75.0),
        ("vibration", "mm/s", 2.5),
        ("pressure", "bar", 6.0),
        ("throughput", "units/hr", 150.0),
    ]
    for _i, (stype, unit, baseline) in enumerate(sensor_types):
        for j in range(5):
            events.append(
                {
                    "event_id": f"evt-{stype}-{j:03d}",
                    "sensor_id": f"sensor-{stype}-{j:03d}",
                    "sensor_type": stype,
                    "facility_id": f"PLANT-{'ABC'[j % 3]}",
                    "timestamp": (base_time - timedelta(minutes=j * 5)).isoformat(),
                    "value": baseline + (j * 0.5),
                    "unit": unit,
                    "quality_score": 0.95 + (j * 0.01),
                    "is_anomaly": j == 4,
                    "metadata": None,
                }
            )
    return events
