"""SLA monitoring for data freshness and completeness.

Monitors data pipeline SLAs including freshness thresholds,
completeness percentages, and alerting for violations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import structlog

from src.config import get_yaml_config

logger = structlog.get_logger(__name__)


class SLAStatus(str, Enum):
    """SLA compliance status."""

    COMPLIANT = "compliant"
    WARNING = "warning"
    VIOLATED = "violated"
    UNKNOWN = "unknown"


@dataclass
class SLACheck:
    """Result of a single SLA check.

    Attributes:
        name: SLA check name.
        status: Compliance status.
        metric_value: Observed metric value.
        threshold: Configured threshold.
        details: Human-readable details.
        checked_at: When the check was performed.
    """

    name: str
    status: SLAStatus
    metric_value: float
    threshold: float
    details: str = ""
    checked_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "name": self.name,
            "status": self.status.value,
            "metric_value": self.metric_value,
            "threshold": self.threshold,
            "details": self.details,
            "checked_at": self.checked_at.isoformat(),
        }


class SLAMonitor:
    """Monitors SLA compliance for the data pipeline.

    Tracks data freshness, completeness, and other quality
    metrics against configured thresholds.

    Attributes:
        freshness_threshold_minutes: Max acceptable data lag in minutes.
        completeness_threshold_percent: Min acceptable data completeness.
        check_history: Historical SLA check results.
    """

    def __init__(self) -> None:
        """Initialize the SLA monitor with configuration."""
        yaml_config = get_yaml_config()
        sla_config = yaml_config.get("governance", {}).get("sla", {})

        self.freshness_threshold_minutes = sla_config.get("freshness_threshold_minutes", 30)
        self.completeness_threshold_percent = sla_config.get("completeness_threshold_percent", 95.0)
        self.alert_channels = sla_config.get("alert_channels", ["log"])
        self.check_history: list[SLACheck] = []

        logger.info(
            "sla_monitor_initialized",
            freshness_threshold=self.freshness_threshold_minutes,
            completeness_threshold=self.completeness_threshold_percent,
        )

    def check_freshness(self, latest_event_time: datetime | None) -> SLACheck:
        """Check data freshness against SLA threshold.

        Args:
            latest_event_time: Timestamp of the most recent event.

        Returns:
            SLACheck result for freshness.
        """
        if latest_event_time is None:
            check = SLACheck(
                name="data_freshness",
                status=SLAStatus.UNKNOWN,
                metric_value=0,
                threshold=self.freshness_threshold_minutes,
                details="No data available to check freshness",
            )
            self.check_history.append(check)
            return check

        lag_minutes = (datetime.utcnow() - latest_event_time).total_seconds() / 60

        if lag_minutes <= self.freshness_threshold_minutes:
            status = SLAStatus.COMPLIANT
        elif lag_minutes <= self.freshness_threshold_minutes * 1.5:
            status = SLAStatus.WARNING
        else:
            status = SLAStatus.VIOLATED

        check = SLACheck(
            name="data_freshness",
            status=status,
            metric_value=round(lag_minutes, 2),
            threshold=self.freshness_threshold_minutes,
            details=(
                f"Data lag: {lag_minutes:.1f} min "
                f"(threshold: {self.freshness_threshold_minutes} min)"
            ),
        )

        self._handle_alert(check)
        self.check_history.append(check)

        logger.info(
            "freshness_check",
            status=status.value,
            lag_minutes=round(lag_minutes, 1),
        )
        return check

    def check_completeness(
        self,
        expected_count: int,
        actual_count: int,
        layer: str = "bronze",
    ) -> SLACheck:
        """Check data completeness against SLA threshold.

        Args:
            expected_count: Expected number of records.
            actual_count: Actual number of records received.
            layer: Data layer being checked.

        Returns:
            SLACheck result for completeness.
        """
        completeness = 100.0 if expected_count == 0 else actual_count / expected_count * 100

        if completeness >= self.completeness_threshold_percent:
            status = SLAStatus.COMPLIANT
        elif completeness >= self.completeness_threshold_percent * 0.9:
            status = SLAStatus.WARNING
        else:
            status = SLAStatus.VIOLATED

        check = SLACheck(
            name=f"data_completeness_{layer}",
            status=status,
            metric_value=round(completeness, 2),
            threshold=self.completeness_threshold_percent,
            details=(
                f"{layer} layer: {actual_count}/{expected_count} records "
                f"({completeness:.1f}%, threshold: {self.completeness_threshold_percent}%)"
            ),
        )

        self._handle_alert(check)
        self.check_history.append(check)

        logger.info(
            "completeness_check",
            layer=layer,
            status=status.value,
            completeness=round(completeness, 1),
        )
        return check

    def _handle_alert(self, check: SLACheck) -> None:
        """Handle alerting for SLA violations.

        Args:
            check: SLACheck result to potentially alert on.
        """
        if check.status in (SLAStatus.WARNING, SLAStatus.VIOLATED):
            for channel in self.alert_channels:
                if channel == "log":
                    logger.warning(
                        "sla_alert",
                        check_name=check.name,
                        status=check.status.value,
                        details=check.details,
                    )

    def get_dashboard_data(self) -> dict[str, Any]:
        """Get SLA monitoring data for dashboard display.

        Returns:
            Dictionary with current SLA status and history.
        """
        recent_checks = self.check_history[-50:] if self.check_history else []

        return {
            "total_checks": len(self.check_history),
            "recent_checks": [c.to_dict() for c in recent_checks],
            "current_status": {
                "freshness": self._latest_check_status("data_freshness"),
                "completeness": self._latest_check_status("data_completeness"),
            },
            "thresholds": {
                "freshness_minutes": self.freshness_threshold_minutes,
                "completeness_percent": self.completeness_threshold_percent,
            },
        }

    def _latest_check_status(self, prefix: str) -> str:
        """Get the latest check status matching a name prefix.

        Args:
            prefix: Check name prefix to match.

        Returns:
            Latest status string, or "unknown".
        """
        for check in reversed(self.check_history):
            if check.name.startswith(prefix):
                return check.status.value
        return SLAStatus.UNKNOWN.value
