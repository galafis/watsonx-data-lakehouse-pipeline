"""Tests for data governance: lineage tracking and SLA monitoring.

Tests cover:
- LineageTracker pipeline lifecycle
- TransformationStep recording and serialization
- LineageGraph history and querying
- SLAMonitor freshness and completeness checks
- SLA alerting and dashboard data
"""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from src.governance.lineage import (
    LayerType,
    LineageGraph,
    LineageTracker,
    TransformationStep,
)
from src.governance.sla_monitor import SLACheck, SLAMonitor, SLAStatus


class TestLayerType:
    """Tests for the LayerType enum."""

    def test_layer_type_values(self) -> None:
        """Verify all layer types are defined correctly."""
        assert LayerType.SOURCE == "source"
        assert LayerType.BRONZE == "bronze"
        assert LayerType.SILVER == "silver"
        assert LayerType.GOLD == "gold"


class TestTransformationStep:
    """Tests for the TransformationStep data class."""

    def test_default_creation(self) -> None:
        """Verify TransformationStep creates with correct defaults."""
        step = TransformationStep()
        assert step.step_id is not None
        assert step.name == ""
        assert step.source_layer == LayerType.SOURCE
        assert step.target_layer == LayerType.BRONZE
        assert step.input_count == 0
        assert step.output_count == 0

    def test_creation_with_values(self) -> None:
        """Verify TransformationStep stores provided values."""
        step = TransformationStep(
            name="deduplicate",
            source_layer=LayerType.BRONZE,
            target_layer=LayerType.SILVER,
            operation="dedup",
            input_count=1000,
            output_count=950,
            metadata={"window": "24h"},
        )
        assert step.name == "deduplicate"
        assert step.input_count == 1000
        assert step.output_count == 950

    def test_to_dict(self) -> None:
        """Verify to_dict produces correct structure."""
        step = TransformationStep(
            name="aggregate",
            source_layer=LayerType.SILVER,
            target_layer=LayerType.GOLD,
            operation="kpi_aggregation",
        )
        d = step.to_dict()

        assert d["name"] == "aggregate"
        assert d["source_layer"] == "silver"
        assert d["target_layer"] == "gold"
        assert "timestamp" in d
        assert "step_id" in d


class TestLineageGraph:
    """Tests for the LineageGraph data class."""

    def test_default_creation(self) -> None:
        """Verify LineageGraph creates with UUID and empty steps."""
        graph = LineageGraph()
        assert graph.pipeline_id is not None
        assert len(graph.steps) == 0
        assert graph.completed_at is None

    def test_to_dict(self) -> None:
        """Verify to_dict produces correct structure."""
        graph = LineageGraph()
        graph.steps.append(TransformationStep(name="step1"))
        d = graph.to_dict()

        assert "pipeline_id" in d
        assert d["total_steps"] == 1
        assert d["completed_at"] is None


class TestLineageTracker:
    """Tests for the LineageTracker class."""

    def test_initialization(self) -> None:
        """Verify LineageTracker starts with no active graph."""
        tracker = LineageTracker()
        assert tracker.current_graph is None
        assert len(tracker.history) == 0

    def test_start_pipeline_creates_graph(self) -> None:
        """Verify start_pipeline creates a new LineageGraph."""
        tracker = LineageTracker()
        pipeline_id = tracker.start_pipeline()

        assert pipeline_id is not None
        assert tracker.current_graph is not None
        assert tracker.current_graph.pipeline_id == pipeline_id

    def test_record_step_appends_to_graph(self) -> None:
        """Verify record_step adds a step to the current graph."""
        tracker = LineageTracker()
        tracker.start_pipeline()

        step = tracker.record_step(
            name="ingest",
            source_layer=LayerType.SOURCE,
            target_layer=LayerType.BRONZE,
            operation="kafka_consume",
            input_count=500,
            output_count=500,
        )

        assert step.name == "ingest"
        assert len(tracker.current_graph.steps) == 1

    def test_record_step_auto_starts_pipeline(self) -> None:
        """Verify record_step starts a pipeline if none is active."""
        tracker = LineageTracker()

        tracker.record_step(
            name="auto_start",
            source_layer=LayerType.BRONZE,
            target_layer=LayerType.SILVER,
            operation="clean",
        )

        assert tracker.current_graph is not None
        assert len(tracker.current_graph.steps) == 1

    def test_complete_pipeline_archives_graph(self) -> None:
        """Verify complete_pipeline moves graph to history."""
        tracker = LineageTracker()
        tracker.start_pipeline()
        tracker.record_step(
            name="step1",
            source_layer=LayerType.SOURCE,
            target_layer=LayerType.BRONZE,
            operation="ingest",
        )

        completed = tracker.complete_pipeline()

        assert completed is not None
        assert completed.completed_at is not None
        assert tracker.current_graph is None
        assert len(tracker.history) == 1

    def test_complete_pipeline_no_active(self) -> None:
        """Verify complete_pipeline returns None when no active pipeline."""
        tracker = LineageTracker()
        result = tracker.complete_pipeline()
        assert result is None

    def test_get_lineage_current(self) -> None:
        """Verify get_lineage returns current graph data."""
        tracker = LineageTracker()
        tracker.start_pipeline()
        tracker.record_step(
            name="step1",
            source_layer=LayerType.BRONZE,
            target_layer=LayerType.SILVER,
            operation="dedup",
        )

        lineage = tracker.get_lineage()
        assert "pipeline_id" in lineage
        assert lineage["total_steps"] == 1

    def test_get_lineage_by_id(self) -> None:
        """Verify get_lineage finds a specific historical pipeline."""
        tracker = LineageTracker()
        pid = tracker.start_pipeline()
        tracker.record_step(
            name="step1",
            source_layer=LayerType.SOURCE,
            target_layer=LayerType.BRONZE,
            operation="ingest",
        )
        tracker.complete_pipeline()

        lineage = tracker.get_lineage(pipeline_id=pid)
        assert lineage["pipeline_id"] == pid

    def test_get_lineage_not_found(self) -> None:
        """Verify get_lineage returns error for unknown pipeline_id."""
        tracker = LineageTracker()
        lineage = tracker.get_lineage(pipeline_id="nonexistent")
        assert "error" in lineage

    def test_get_lineage_no_active(self) -> None:
        """Verify get_lineage returns error when no active pipeline."""
        tracker = LineageTracker()
        lineage = tracker.get_lineage()
        assert "error" in lineage

    def test_get_full_lineage(self) -> None:
        """Verify get_full_lineage returns all historical graphs."""
        tracker = LineageTracker()

        tracker.start_pipeline()
        tracker.complete_pipeline()
        tracker.start_pipeline()
        tracker.complete_pipeline()

        graphs = tracker.get_full_lineage()
        assert len(graphs) == 2


class TestSLAStatus:
    """Tests for the SLAStatus enum."""

    def test_status_values(self) -> None:
        """Verify all SLA status values are defined."""
        assert SLAStatus.COMPLIANT == "compliant"
        assert SLAStatus.WARNING == "warning"
        assert SLAStatus.VIOLATED == "violated"
        assert SLAStatus.UNKNOWN == "unknown"


class TestSLACheck:
    """Tests for the SLACheck data class."""

    def test_to_dict(self) -> None:
        """Verify to_dict produces correct structure."""
        check = SLACheck(
            name="data_freshness",
            status=SLAStatus.COMPLIANT,
            metric_value=15.5,
            threshold=30.0,
            details="Data lag: 15.5 min",
        )
        d = check.to_dict()

        assert d["name"] == "data_freshness"
        assert d["status"] == "compliant"
        assert d["metric_value"] == 15.5
        assert d["threshold"] == 30.0


class TestSLAMonitor:
    """Tests for the SLAMonitor class."""

    @patch("src.governance.sla_monitor.get_yaml_config")
    def test_initialization(self, mock_yaml: MagicMock) -> None:
        """Verify SLAMonitor loads configuration correctly."""
        mock_yaml.return_value = {
            "governance": {
                "sla": {
                    "freshness_threshold_minutes": 30,
                    "completeness_threshold_percent": 95.0,
                    "alert_channels": ["log"],
                }
            }
        }
        monitor = SLAMonitor()

        assert monitor.freshness_threshold_minutes == 30
        assert monitor.completeness_threshold_percent == 95.0
        assert monitor.alert_channels == ["log"]

    @patch("src.governance.sla_monitor.get_yaml_config")
    def test_check_freshness_compliant(self, mock_yaml: MagicMock) -> None:
        """Verify freshness check returns COMPLIANT for recent data."""
        mock_yaml.return_value = {"governance": {"sla": {"freshness_threshold_minutes": 30}}}
        monitor = SLAMonitor()

        recent_time = datetime.utcnow() - timedelta(minutes=10)
        check = monitor.check_freshness(recent_time)

        assert check.status == SLAStatus.COMPLIANT
        assert check.name == "data_freshness"

    @patch("src.governance.sla_monitor.get_yaml_config")
    def test_check_freshness_warning(self, mock_yaml: MagicMock) -> None:
        """Verify freshness check returns WARNING for slightly stale data."""
        mock_yaml.return_value = {"governance": {"sla": {"freshness_threshold_minutes": 30}}}
        monitor = SLAMonitor()

        stale_time = datetime.utcnow() - timedelta(minutes=40)
        check = monitor.check_freshness(stale_time)

        assert check.status == SLAStatus.WARNING

    @patch("src.governance.sla_monitor.get_yaml_config")
    def test_check_freshness_violated(self, mock_yaml: MagicMock) -> None:
        """Verify freshness check returns VIOLATED for very stale data."""
        mock_yaml.return_value = {"governance": {"sla": {"freshness_threshold_minutes": 30}}}
        monitor = SLAMonitor()

        very_stale = datetime.utcnow() - timedelta(hours=2)
        check = monitor.check_freshness(very_stale)

        assert check.status == SLAStatus.VIOLATED

    @patch("src.governance.sla_monitor.get_yaml_config")
    def test_check_freshness_unknown(self, mock_yaml: MagicMock) -> None:
        """Verify freshness check returns UNKNOWN for None timestamp."""
        mock_yaml.return_value = {"governance": {"sla": {"freshness_threshold_minutes": 30}}}
        monitor = SLAMonitor()

        check = monitor.check_freshness(None)
        assert check.status == SLAStatus.UNKNOWN

    @patch("src.governance.sla_monitor.get_yaml_config")
    def test_check_completeness_compliant(self, mock_yaml: MagicMock) -> None:
        """Verify completeness check returns COMPLIANT above threshold."""
        mock_yaml.return_value = {"governance": {"sla": {"completeness_threshold_percent": 95.0}}}
        monitor = SLAMonitor()

        check = monitor.check_completeness(expected_count=1000, actual_count=980)

        assert check.status == SLAStatus.COMPLIANT
        assert check.metric_value == 98.0

    @patch("src.governance.sla_monitor.get_yaml_config")
    def test_check_completeness_warning(self, mock_yaml: MagicMock) -> None:
        """Verify completeness check returns WARNING near threshold."""
        mock_yaml.return_value = {"governance": {"sla": {"completeness_threshold_percent": 95.0}}}
        monitor = SLAMonitor()

        check = monitor.check_completeness(expected_count=1000, actual_count=870)

        assert check.status == SLAStatus.WARNING

    @patch("src.governance.sla_monitor.get_yaml_config")
    def test_check_completeness_violated(self, mock_yaml: MagicMock) -> None:
        """Verify completeness check returns VIOLATED below threshold."""
        mock_yaml.return_value = {"governance": {"sla": {"completeness_threshold_percent": 95.0}}}
        monitor = SLAMonitor()

        check = monitor.check_completeness(expected_count=1000, actual_count=500)

        assert check.status == SLAStatus.VIOLATED

    @patch("src.governance.sla_monitor.get_yaml_config")
    def test_check_completeness_zero_expected(self, mock_yaml: MagicMock) -> None:
        """Verify completeness is 100% when expected_count is 0."""
        mock_yaml.return_value = {"governance": {"sla": {"completeness_threshold_percent": 95.0}}}
        monitor = SLAMonitor()

        check = monitor.check_completeness(expected_count=0, actual_count=0)

        assert check.status == SLAStatus.COMPLIANT
        assert check.metric_value == 100.0

    @patch("src.governance.sla_monitor.get_yaml_config")
    def test_check_history_maintained(self, mock_yaml: MagicMock) -> None:
        """Verify check results are stored in history."""
        mock_yaml.return_value = {"governance": {"sla": {"freshness_threshold_minutes": 30}}}
        monitor = SLAMonitor()

        monitor.check_freshness(datetime.utcnow())
        monitor.check_completeness(100, 95)

        assert len(monitor.check_history) == 2

    @patch("src.governance.sla_monitor.get_yaml_config")
    def test_get_dashboard_data(self, mock_yaml: MagicMock) -> None:
        """Verify get_dashboard_data returns structured data."""
        mock_yaml.return_value = {
            "governance": {
                "sla": {
                    "freshness_threshold_minutes": 30,
                    "completeness_threshold_percent": 95.0,
                }
            }
        }
        monitor = SLAMonitor()

        monitor.check_freshness(datetime.utcnow())
        monitor.check_completeness(100, 98, layer="bronze")

        dashboard = monitor.get_dashboard_data()

        assert "total_checks" in dashboard
        assert dashboard["total_checks"] == 2
        assert "current_status" in dashboard
        assert "thresholds" in dashboard
        assert dashboard["thresholds"]["freshness_minutes"] == 30

    @patch("src.governance.sla_monitor.get_yaml_config")
    def test_latest_check_status_unknown(self, mock_yaml: MagicMock) -> None:
        """Verify _latest_check_status returns unknown for empty history."""
        mock_yaml.return_value = {"governance": {"sla": {}}}
        monitor = SLAMonitor()

        status = monitor._latest_check_status("data_freshness")
        assert status == "unknown"
