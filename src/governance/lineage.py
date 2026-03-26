"""Data lineage tracking across bronze, silver, and gold transformations.

Tracks data flow, transformation metadata, and dependency graphs
across the medallion architecture layers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import uuid4

import structlog

logger = structlog.get_logger(__name__)


class LayerType(str, Enum):
    """Data lakehouse layer types."""

    SOURCE = "source"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass
class TransformationStep:
    """Represents a single transformation in the data pipeline.

    Attributes:
        step_id: Unique step identifier.
        name: Human-readable transformation name.
        source_layer: Input layer.
        target_layer: Output layer.
        operation: Type of operation performed.
        timestamp: When the transformation was executed.
        input_count: Number of input rows.
        output_count: Number of output rows.
        metadata: Additional transformation metadata.
    """

    step_id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    source_layer: LayerType = LayerType.SOURCE
    target_layer: LayerType = LayerType.BRONZE
    operation: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    input_count: int = 0
    output_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "step_id": self.step_id,
            "name": self.name,
            "source_layer": self.source_layer.value,
            "target_layer": self.target_layer.value,
            "operation": self.operation,
            "timestamp": self.timestamp.isoformat(),
            "input_count": self.input_count,
            "output_count": self.output_count,
            "metadata": self.metadata,
        }


@dataclass
class LineageGraph:
    """Data lineage graph tracking transformations across layers.

    Attributes:
        pipeline_id: Unique pipeline execution ID.
        steps: Ordered list of transformation steps.
        started_at: Pipeline start time.
        completed_at: Pipeline completion time.
    """

    pipeline_id: str = field(default_factory=lambda: str(uuid4()))
    steps: list[TransformationStep] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "pipeline_id": self.pipeline_id,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "total_steps": len(self.steps),
            "steps": [s.to_dict() for s in self.steps],
        }


class LineageTracker:
    """Tracks data lineage across the lakehouse pipeline.

    Records transformation steps, maintains a lineage graph,
    and provides query capabilities for auditing.

    Attributes:
        current_graph: Active lineage graph being tracked.
        history: Historical lineage graphs.
    """

    def __init__(self) -> None:
        """Initialize the lineage tracker."""
        self.current_graph: LineageGraph | None = None
        self.history: list[LineageGraph] = []
        logger.info("lineage_tracker_initialized")

    def start_pipeline(self) -> str:
        """Start tracking a new pipeline execution.

        Returns:
            Pipeline execution ID.
        """
        self.current_graph = LineageGraph()
        logger.info("pipeline_tracking_started", pipeline_id=self.current_graph.pipeline_id)
        return self.current_graph.pipeline_id

    def record_step(
        self,
        name: str,
        source_layer: LayerType,
        target_layer: LayerType,
        operation: str,
        input_count: int = 0,
        output_count: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> TransformationStep:
        """Record a transformation step in the lineage graph.

        Args:
            name: Human-readable step name.
            source_layer: Input data layer.
            target_layer: Output data layer.
            operation: Type of operation (e.g., "deduplicate", "aggregate").
            input_count: Number of input rows.
            output_count: Number of output rows.
            metadata: Additional metadata.

        Returns:
            The recorded TransformationStep.
        """
        if self.current_graph is None:
            self.start_pipeline()

        step = TransformationStep(
            name=name,
            source_layer=source_layer,
            target_layer=target_layer,
            operation=operation,
            input_count=input_count,
            output_count=output_count,
            metadata=metadata or {},
        )

        assert self.current_graph is not None
        self.current_graph.steps.append(step)

        logger.info(
            "lineage_step_recorded",
            step=name,
            source=source_layer.value,
            target=target_layer.value,
            operation=operation,
            input_count=input_count,
            output_count=output_count,
        )
        return step

    def complete_pipeline(self) -> LineageGraph | None:
        """Mark the current pipeline as completed.

        Returns:
            The completed LineageGraph, or None if no active pipeline.
        """
        if self.current_graph is None:
            return None

        self.current_graph.completed_at = datetime.utcnow()
        completed = self.current_graph
        self.history.append(completed)
        self.current_graph = None

        logger.info(
            "pipeline_tracking_completed",
            pipeline_id=completed.pipeline_id,
            total_steps=len(completed.steps),
        )
        return completed

    def get_lineage(self, pipeline_id: str | None = None) -> dict[str, Any]:
        """Get lineage information for a pipeline.

        Args:
            pipeline_id: Specific pipeline ID, or None for current.

        Returns:
            Lineage graph as dictionary.
        """
        if pipeline_id:
            for graph in self.history:
                if graph.pipeline_id == pipeline_id:
                    return graph.to_dict()
            return {"error": f"Pipeline {pipeline_id} not found"}

        if self.current_graph:
            return self.current_graph.to_dict()

        return {"error": "No active pipeline"}

    def get_full_lineage(self) -> list[dict[str, Any]]:
        """Get all historical lineage graphs.

        Returns:
            List of lineage graphs as dictionaries.
        """
        graphs = [g.to_dict() for g in self.history]
        if self.current_graph:
            graphs.append(self.current_graph.to_dict())
        return graphs
