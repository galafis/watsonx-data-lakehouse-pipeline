"""Schema definitions for IoT sensor events.

Provides PySpark StructType schemas and Pydantic models for
temperature, vibration, pressure, and throughput sensor events.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from pyspark.sql.types import StructType
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)


class SensorType(str, Enum):
    """Supported IoT sensor types."""

    TEMPERATURE = "temperature"
    VIBRATION = "vibration"
    PRESSURE = "pressure"
    THROUGHPUT = "throughput"


class IoTEvent(BaseModel):
    """Pydantic model for an IoT sensor event."""

    event_id: str = Field(description="Unique event identifier (UUID)")
    sensor_id: str = Field(description="Unique sensor identifier")
    sensor_type: SensorType = Field(description="Type of sensor")
    facility_id: str = Field(description="Facility where the sensor is located")
    timestamp: datetime = Field(description="Event timestamp in UTC")
    value: float = Field(description="Sensor reading value")
    unit: str = Field(description="Measurement unit")
    quality_score: float = Field(default=1.0, ge=0.0, le=1.0, description="Data quality score")
    is_anomaly: bool = Field(default=False, description="Whether this reading is anomalous")
    metadata: dict[str, str] | None = Field(default=None, description="Additional metadata")


def get_spark_schema() -> StructType:
    """Get PySpark StructType schema for IoT events.

    Returns:
        PySpark StructType defining the IoT event schema.
    """
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        MapType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("event_id", StringType(), nullable=False),
            StructField("sensor_id", StringType(), nullable=False),
            StructField("sensor_type", StringType(), nullable=False),
            StructField("facility_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("value", DoubleType(), nullable=False),
            StructField("unit", StringType(), nullable=False),
            StructField("quality_score", DoubleType(), nullable=True),
            StructField("is_anomaly", BooleanType(), nullable=True),
            StructField("metadata", MapType(StringType(), StringType()), nullable=True),
        ]
    )

    logger.debug("spark_schema_created", num_fields=len(schema.fields))
    return schema


def validate_event(event_dict: dict) -> IoTEvent | None:
    """Validate a raw event dictionary against the IoT event schema.

    Args:
        event_dict: Raw event dictionary from Kafka.

    Returns:
        Validated IoTEvent or None if validation fails.
    """
    try:
        return IoTEvent(**event_dict)
    except Exception as exc:
        logger.warning(
            "event_validation_failed", error=str(exc), event_id=event_dict.get("event_id")
        )
        return None
