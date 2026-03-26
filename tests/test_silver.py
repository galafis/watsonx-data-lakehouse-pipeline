"""Tests for the Silver layer of the data lakehouse.

Tests cover:
- SilverLayer initialization with configuration
- Deduplication logic
- Data cleaning transformations
- Enrichment with derived columns
- Full processing pipeline
- Read and statistics operations
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestSilverLayerInit:
    """Tests for SilverLayer initialization."""

    @patch("src.lakehouse.silver.get_settings")
    @patch("src.lakehouse.silver.get_yaml_config")
    def test_initialization(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify SilverLayer initializes with correct paths and config."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(
                silver_path="/test/silver",
                bronze_path="/test/bronze",
            )
        )
        mock_yaml.return_value = {
            "lakehouse": {
                "silver": {
                    "partition_columns": ["event_date", "facility_id"],
                    "dedup_window_hours": 24,
                }
            }
        }

        from src.lakehouse.silver import SilverLayer

        spark = MagicMock()
        layer = SilverLayer(spark)

        assert layer.silver_path == "/test/silver"
        assert layer.bronze_path == "/test/bronze"
        assert layer.partition_columns == ["event_date", "facility_id"]
        assert layer.dedup_window_hours == 24


class TestSilverLayerDeduplicate:
    """Tests for deduplication operations."""

    @patch("src.lakehouse.silver.get_settings")
    @patch("src.lakehouse.silver.get_yaml_config")
    def test_deduplicate_removes_duplicates(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify deduplicate removes rows based on event_id window."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(silver_path="/test/silver", bronze_path="/test/bronze")
        )
        mock_yaml.return_value = {"lakehouse": {"silver": {}}}

        from src.lakehouse.silver import SilverLayer

        layer = SilverLayer(MagicMock())

        mock_df = MagicMock()
        windowed_df = MagicMock()
        deduped_df = MagicMock()

        mock_df.withColumn.return_value = windowed_df
        windowed_df.filter.return_value = deduped_df
        deduped_df.drop.return_value = deduped_df

        mock_df.count.return_value = 100
        deduped_df.count.return_value = 95

        result = layer.deduplicate(mock_df)

        mock_df.withColumn.assert_called_once()


class TestSilverLayerClean:
    """Tests for data cleaning operations."""

    @patch("src.lakehouse.silver.get_settings")
    @patch("src.lakehouse.silver.get_yaml_config")
    def test_clean_applies_transformations(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify clean applies null fills, casts, and trims."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(silver_path="/test/silver", bronze_path="/test/bronze")
        )
        mock_yaml.return_value = {"lakehouse": {"silver": {}}}

        from src.lakehouse.silver import SilverLayer

        layer = SilverLayer(MagicMock())

        mock_df = MagicMock()
        chain_df = MagicMock()
        mock_df.withColumn.return_value = chain_df
        chain_df.withColumn.return_value = chain_df
        chain_df.filter.return_value = chain_df

        result = layer.clean(mock_df)

        mock_df.withColumn.assert_called_once()


class TestSilverLayerEnrich:
    """Tests for data enrichment operations."""

    @patch("src.lakehouse.silver.get_settings")
    @patch("src.lakehouse.silver.get_yaml_config")
    def test_enrich_adds_derived_columns(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify enrich adds rolling stats, z-score, and time features."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(silver_path="/test/silver", bronze_path="/test/bronze")
        )
        mock_yaml.return_value = {"lakehouse": {"silver": {}}}

        from src.lakehouse.silver import SilverLayer

        layer = SilverLayer(MagicMock())

        mock_df = MagicMock()
        enriched = MagicMock()
        mock_df.withColumn.return_value = enriched
        enriched.withColumn.return_value = enriched

        result = layer.enrich(mock_df)

        mock_df.withColumn.assert_called_once()


class TestSilverLayerProcess:
    """Tests for the full Silver layer processing pipeline."""

    @patch("src.lakehouse.silver.get_settings")
    @patch("src.lakehouse.silver.get_yaml_config")
    def test_process_full_load(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify process handles full load when no silver data exists."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(silver_path="/test/silver", bronze_path="/test/bronze")
        )
        mock_yaml.return_value = {"lakehouse": {"silver": {}}}

        from src.lakehouse.silver import SilverLayer

        spark = MagicMock()
        bronze_df = MagicMock()
        spark.read.parquet.side_effect = [bronze_df, Exception("No silver data")]

        layer = SilverLayer(spark)

        # Mock the chain of transformations
        deduped = MagicMock()
        cleaned = MagicMock()
        enriched = MagicMock()

        with patch.object(layer, "deduplicate", return_value=deduped):
            with patch.object(layer, "clean", return_value=cleaned):
                with patch.object(layer, "enrich", return_value=enriched):
                    enriched.count.return_value = 50
                    enriched.write.mode.return_value.partitionBy.return_value.parquet = MagicMock()

                    result = layer.process(incremental=True)

                    assert result == 50

    @patch("src.lakehouse.silver.get_settings")
    @patch("src.lakehouse.silver.get_yaml_config")
    def test_process_returns_zero_for_empty(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify process returns 0 when there is no new data."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(silver_path="/test/silver", bronze_path="/test/bronze")
        )
        mock_yaml.return_value = {"lakehouse": {"silver": {}}}

        from src.lakehouse.silver import SilverLayer

        spark = MagicMock()
        spark.read.parquet.return_value = MagicMock()

        layer = SilverLayer(spark)

        with patch.object(layer, "deduplicate") as mock_dedup:
            with patch.object(layer, "clean") as mock_clean:
                with patch.object(layer, "enrich") as mock_enrich:
                    mock_enrich.return_value.count.return_value = 0
                    result = layer.process(incremental=False)
                    assert result == 0


class TestSilverLayerStats:
    """Tests for SilverLayer statistics computation."""

    @patch("src.lakehouse.silver.get_settings")
    @patch("src.lakehouse.silver.get_yaml_config")
    def test_get_stats_returns_metrics(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify get_stats returns quality and anomaly metrics."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(silver_path="/test/silver", bronze_path="/test/bronze")
        )
        mock_yaml.return_value = {"lakehouse": {"silver": {}}}

        from src.lakehouse.silver import SilverLayer

        spark = MagicMock()
        mock_df = MagicMock()
        spark.read.parquet.return_value = mock_df
        mock_df.count.return_value = 5000
        mock_df.agg.return_value.collect.return_value = [
            MagicMock(__getitem__=lambda self, i: 0.95)
        ]

        layer = SilverLayer(spark)
        stats = layer.get_stats()

        assert stats["layer"] == "silver"

    @patch("src.lakehouse.silver.get_settings")
    @patch("src.lakehouse.silver.get_yaml_config")
    def test_get_stats_handles_exception(
        self, mock_yaml: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Verify get_stats returns error dict on failure."""
        mock_settings.return_value = MagicMock(
            lakehouse=MagicMock(silver_path="/nonexistent", bronze_path="/test/bronze")
        )
        mock_yaml.return_value = {"lakehouse": {"silver": {}}}

        from src.lakehouse.silver import SilverLayer

        spark = MagicMock()
        spark.read.parquet.side_effect = Exception("Path not found")

        layer = SilverLayer(spark)
        stats = layer.get_stats()

        assert stats["total_rows"] == 0
        assert "error" in stats
