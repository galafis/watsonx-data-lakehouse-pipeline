"""Tests for the Bronze layer of the data lakehouse.

Tests cover:
- BronzeLayer initialization with configuration
- Batch ingestion with metadata enrichment
- Read operations with date and sensor type filters
- Statistics computation
- Expired data cleanup
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestBronzeLayerInit:
    """Tests for BronzeLayer initialization."""

    @patch("src.lakehouse.bronze.get_settings")
    @patch("src.lakehouse.bronze.get_yaml_config")
    def test_initialization_with_config(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify BronzeLayer initializes with correct path and partition columns."""
        mock_settings.return_value = MagicMock(lakehouse=MagicMock(bronze_path="/test/bronze"))
        mock_yaml.return_value = {
            "lakehouse": {
                "bronze": {
                    "partition_columns": ["event_date", "sensor_type"],
                    "retention_days": 90,
                }
            }
        }

        from src.lakehouse.bronze import BronzeLayer

        spark = MagicMock()
        layer = BronzeLayer(spark)

        assert layer.bronze_path == "/test/bronze"
        assert layer.partition_columns == ["event_date", "sensor_type"]
        assert layer.retention_days == 90
        assert layer.spark is spark

    @patch("src.lakehouse.bronze.get_settings")
    @patch("src.lakehouse.bronze.get_yaml_config")
    def test_initialization_with_defaults(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify BronzeLayer uses defaults when YAML config is empty."""
        mock_settings.return_value = MagicMock(lakehouse=MagicMock(bronze_path="/default/bronze"))
        mock_yaml.return_value = {}

        from src.lakehouse.bronze import BronzeLayer

        layer = BronzeLayer(MagicMock())
        assert layer.partition_columns == ["event_date", "sensor_type"]
        assert layer.retention_days == 90


class TestBronzeLayerIngest:
    """Tests for BronzeLayer ingestion operations."""

    @patch("src.lakehouse.bronze.get_settings")
    @patch("src.lakehouse.bronze.get_yaml_config")
    def test_ingest_batch_adds_metadata_columns(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify ingest_batch enriches data with ingestion metadata."""
        mock_settings.return_value = MagicMock(lakehouse=MagicMock(bronze_path="/test/bronze"))
        mock_yaml.return_value = {"lakehouse": {"bronze": {}}}

        from src.lakehouse.bronze import BronzeLayer

        spark = MagicMock()
        layer = BronzeLayer(spark)

        mock_df = MagicMock()
        enriched_df = MagicMock()
        mock_df.withColumn.return_value = enriched_df
        enriched_df.withColumn.return_value = enriched_df
        enriched_df.count.return_value = 42
        enriched_df.write.mode.return_value.partitionBy.return_value.parquet = MagicMock()

        result = layer.ingest_batch(mock_df)

        assert result == 42
        mock_df.withColumn.assert_called_once()

    @patch("src.lakehouse.bronze.get_settings")
    @patch("src.lakehouse.bronze.get_yaml_config")
    def test_ingest_batch_writes_partitioned_parquet(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify ingest_batch writes to partitioned Parquet format."""
        mock_settings.return_value = MagicMock(lakehouse=MagicMock(bronze_path="/test/bronze"))
        mock_yaml.return_value = {"lakehouse": {"bronze": {}}}

        from src.lakehouse.bronze import BronzeLayer

        layer = BronzeLayer(MagicMock())

        mock_df = MagicMock()
        enriched = MagicMock()
        mock_df.withColumn.return_value = enriched
        enriched.withColumn.return_value = enriched
        enriched.count.return_value = 10
        write_mock = enriched.write.mode.return_value.partitionBy.return_value
        write_mock.parquet = MagicMock()

        layer.ingest_batch(mock_df)

        enriched.write.mode.assert_called_with("append")


class TestBronzeLayerRead:
    """Tests for BronzeLayer read operations."""

    @patch("src.lakehouse.bronze.get_settings")
    @patch("src.lakehouse.bronze.get_yaml_config")
    def test_read_without_filters(self, mock_yaml: MagicMock, mock_settings: MagicMock) -> None:
        """Verify read returns full DataFrame when no filters are specified."""
        mock_settings.return_value = MagicMock(lakehouse=MagicMock(bronze_path="/test/bronze"))
        mock_yaml.return_value = {"lakehouse": {"bronze": {}}}

        from src.lakehouse.bronze import BronzeLayer

        spark = MagicMock()
        layer = BronzeLayer(spark)

        layer.read()

        spark.read.parquet.assert_called_once_with("/test/bronze")

    @patch("src.lakehouse.bronze.get_settings")
    @patch("src.lakehouse.bronze.get_yaml_config")
    def test_read_with_date_filter(self, mock_yaml: MagicMock, mock_settings: MagicMock) -> None:
        """Verify read applies date filters correctly."""
        mock_settings.return_value = MagicMock(lakehouse=MagicMock(bronze_path="/test/bronze"))
        mock_yaml.return_value = {"lakehouse": {"bronze": {}}}

        from src.lakehouse.bronze import BronzeLayer

        spark = MagicMock()
        mock_df = MagicMock()
        spark.read.parquet.return_value = mock_df
        mock_df.filter.return_value = mock_df

        layer = BronzeLayer(spark)
        layer.read(start_date="2024-01-01", end_date="2024-12-31")

        assert mock_df.filter.call_count == 2

    @patch("src.lakehouse.bronze.get_settings")
    @patch("src.lakehouse.bronze.get_yaml_config")
    def test_read_with_sensor_type_filter(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify read applies sensor type filter correctly."""
        mock_settings.return_value = MagicMock(lakehouse=MagicMock(bronze_path="/test/bronze"))
        mock_yaml.return_value = {"lakehouse": {"bronze": {}}}

        from src.lakehouse.bronze import BronzeLayer

        spark = MagicMock()
        mock_df = MagicMock()
        spark.read.parquet.return_value = mock_df
        mock_df.filter.return_value = mock_df

        layer = BronzeLayer(spark)
        layer.read(sensor_type="temperature")

        mock_df.filter.assert_called_once()


class TestBronzeLayerStats:
    """Tests for BronzeLayer statistics computation."""

    @patch("src.lakehouse.bronze.get_settings")
    @patch("src.lakehouse.bronze.get_yaml_config")
    def test_get_stats_returns_dict(self, mock_yaml: MagicMock, mock_settings: MagicMock) -> None:
        """Verify get_stats returns a dictionary with expected keys."""
        mock_settings.return_value = MagicMock(lakehouse=MagicMock(bronze_path="/test/bronze"))
        mock_yaml.return_value = {"lakehouse": {"bronze": {}}}

        from src.lakehouse.bronze import BronzeLayer

        spark = MagicMock()
        mock_df = MagicMock()
        spark.read.parquet.return_value = mock_df
        mock_df.count.return_value = 1000
        mock_df.agg.return_value.collect.return_value = [
            MagicMock(__getitem__=lambda self, i: "2024-01-01")
        ]
        mock_df.groupBy.return_value.count.return_value.collect.return_value = [
            {"sensor_type": "temperature", "count": 500},
            {"sensor_type": "vibration", "count": 500},
        ]

        layer = BronzeLayer(spark)
        stats = layer.get_stats()

        assert stats["layer"] == "bronze"

    @patch("src.lakehouse.bronze.get_settings")
    @patch("src.lakehouse.bronze.get_yaml_config")
    def test_get_stats_handles_exception(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify get_stats returns error dict on failure."""
        mock_settings.return_value = MagicMock(lakehouse=MagicMock(bronze_path="/nonexistent"))
        mock_yaml.return_value = {"lakehouse": {"bronze": {}}}

        from src.lakehouse.bronze import BronzeLayer

        spark = MagicMock()
        spark.read.parquet.side_effect = Exception("Path not found")

        layer = BronzeLayer(spark)
        stats = layer.get_stats()

        assert stats["total_rows"] == 0
        assert "error" in stats

    @patch("src.lakehouse.bronze.get_settings")
    @patch("src.lakehouse.bronze.get_yaml_config")
    def test_cleanup_expired_returns_zero(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify cleanup_expired returns 0 (safety mode)."""
        mock_settings.return_value = MagicMock(lakehouse=MagicMock(bronze_path="/test/bronze"))
        mock_yaml.return_value = {"lakehouse": {"bronze": {}}}

        from src.lakehouse.bronze import BronzeLayer

        layer = BronzeLayer(MagicMock())
        assert layer.cleanup_expired() == 0
