"""Tests for the Gold layer of the data lakehouse.

Tests cover:
- GoldLayer initialization with configuration
- KPI aggregation logic
- Anomaly score computation
- Full processing pipeline
- Read and statistics operations
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestGoldLayerInit:
    """Tests for GoldLayer initialization."""

    @patch("src.lakehouse.gold.get_settings")
    @patch("src.lakehouse.gold.get_yaml_config")
    def test_initialization(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify GoldLayer initializes with correct paths and config."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(
                gold_path="/test/gold",
                silver_path="/test/silver",
            )
        )
        mock_yaml.return_value = {
            "lakehouse": {
                "gold": {
                    "partition_columns": ["kpi_date", "facility_id"],
                    "aggregation_window_minutes": 15,
                }
            }
        }

        from src.lakehouse.gold import GoldLayer

        spark = MagicMock()
        layer = GoldLayer(spark)

        assert layer.gold_path == "/test/gold"
        assert layer.silver_path == "/test/silver"
        assert layer.partition_columns == ["kpi_date", "facility_id"]
        assert layer.aggregation_window == 15


class TestGoldLayerAggregateKPIs:
    """Tests for KPI aggregation."""

    @patch("src.lakehouse.gold.get_settings")
    @patch("src.lakehouse.gold.get_yaml_config")
    def test_aggregate_kpis_applies_window(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify aggregate_kpis applies time window grouping."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(gold_path="/test/gold", silver_path="/test/silver")
        )
        mock_yaml.return_value = {"lakehouse": {"gold": {}}}

        from src.lakehouse.gold import GoldLayer

        layer = GoldLayer(MagicMock())

        mock_df = MagicMock()
        windowed = MagicMock()
        grouped = MagicMock()
        agg_result = MagicMock()

        mock_df.withColumn.return_value = windowed
        windowed.groupBy.return_value = grouped
        grouped.agg.return_value = agg_result
        agg_result.withColumn.return_value = agg_result
        agg_result.drop.return_value = agg_result

        result = layer.aggregate_kpis(mock_df)

        mock_df.withColumn.assert_called_once()


class TestGoldLayerAnomalyScores:
    """Tests for anomaly score computation."""

    @patch("src.lakehouse.gold.get_settings")
    @patch("src.lakehouse.gold.get_yaml_config")
    def test_compute_anomaly_scores(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify anomaly scoring computes composite scores with severity levels."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(gold_path="/test/gold", silver_path="/test/silver")
        )
        mock_yaml.return_value = {"lakehouse": {"gold": {}}}

        from src.lakehouse.gold import GoldLayer

        layer = GoldLayer(MagicMock())

        mock_df = MagicMock()
        grouped = MagicMock()
        agg_result = MagicMock()

        mock_df.groupBy.return_value = grouped
        grouped.agg.return_value = agg_result
        agg_result.withColumn.return_value = agg_result

        result = layer.compute_anomaly_scores(mock_df)

        mock_df.groupBy.assert_called_once()


class TestGoldLayerProcess:
    """Tests for the full Gold layer processing pipeline."""

    @patch("src.lakehouse.gold.get_settings")
    @patch("src.lakehouse.gold.get_yaml_config")
    def test_process_writes_kpis_and_anomalies(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify process writes both KPIs and anomaly scores."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(gold_path="/test/gold", silver_path="/test/silver")
        )
        mock_yaml.return_value = {"lakehouse": {"gold": {}}}

        from src.lakehouse.gold import GoldLayer

        spark = MagicMock()
        silver_df = MagicMock()
        spark.read.parquet.return_value = silver_df

        layer = GoldLayer(spark)

        mock_kpis = MagicMock()
        mock_anomalies = MagicMock()
        mock_kpis.count.return_value = 120

        with patch.object(layer, "aggregate_kpis", return_value=mock_kpis):
            with patch.object(layer, "compute_anomaly_scores", return_value=mock_anomalies):
                result = layer.process()

                assert result == 120
                mock_kpis.write.mode.assert_called_with("overwrite")
                mock_anomalies.write.mode.assert_called_with("overwrite")


class TestGoldLayerRead:
    """Tests for GoldLayer read operations."""

    @patch("src.lakehouse.gold.get_settings")
    @patch("src.lakehouse.gold.get_yaml_config")
    def test_read_kpis_without_filters(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify read_kpis returns full DataFrame when no filters specified."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(gold_path="/test/gold", silver_path="/test/silver")
        )
        mock_yaml.return_value = {"lakehouse": {"gold": {}}}

        from src.lakehouse.gold import GoldLayer

        spark = MagicMock()
        layer = GoldLayer(spark)

        layer.read_kpis()
        spark.read.parquet.assert_called_with("/test/gold/kpis")

    @patch("src.lakehouse.gold.get_settings")
    @patch("src.lakehouse.gold.get_yaml_config")
    def test_read_kpis_with_facility_filter(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify read_kpis applies facility_id filter."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(gold_path="/test/gold", silver_path="/test/silver")
        )
        mock_yaml.return_value = {"lakehouse": {"gold": {}}}

        from src.lakehouse.gold import GoldLayer

        spark = MagicMock()
        mock_df = MagicMock()
        spark.read.parquet.return_value = mock_df
        mock_df.filter.return_value = mock_df

        layer = GoldLayer(spark)
        layer.read_kpis(facility_id="PLANT-A")

        mock_df.filter.assert_called_once()

    @patch("src.lakehouse.gold.get_settings")
    @patch("src.lakehouse.gold.get_yaml_config")
    def test_read_anomaly_scores(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify read_anomaly_scores reads from correct path."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(gold_path="/test/gold", silver_path="/test/silver")
        )
        mock_yaml.return_value = {"lakehouse": {"gold": {}}}

        from src.lakehouse.gold import GoldLayer

        spark = MagicMock()
        layer = GoldLayer(spark)

        layer.read_anomaly_scores()
        spark.read.parquet.assert_called_with("/test/gold/anomaly_scores")


class TestGoldLayerStats:
    """Tests for GoldLayer statistics computation."""

    @patch("src.lakehouse.gold.get_settings")
    @patch("src.lakehouse.gold.get_yaml_config")
    def test_get_stats_handles_exception(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify get_stats returns error dict on failure."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(gold_path="/nonexistent", silver_path="/test/silver")
        )
        mock_yaml.return_value = {"lakehouse": {"gold": {}}}

        from src.lakehouse.gold import GoldLayer

        spark = MagicMock()
        spark.read.parquet.side_effect = Exception("Path not found")

        layer = GoldLayer(spark)
        stats = layer.get_stats()

        assert stats["total_kpi_rows"] == 0
        assert "error" in stats
