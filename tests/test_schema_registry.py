"""Tests for the IoT schema registry and event validation.

Tests cover:
- SensorType enum values
- IoTEvent Pydantic model validation
- PySpark schema creation
- Event validation with valid and invalid data
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.schema_registry import IoTEvent, SensorType, validate_event


class TestSensorType:
    """Tests for the SensorType enum."""

    def test_sensor_type_values(self) -> None:
        """Verify all expected sensor types are defined."""
        assert SensorType.TEMPERATURE == "temperature"
        assert SensorType.VIBRATION == "vibration"
        assert SensorType.PRESSURE == "pressure"
        assert SensorType.THROUGHPUT == "throughput"

    def test_sensor_type_count(self) -> None:
        """Verify exactly four sensor types exist."""
        assert len(SensorType) == 4

    def test_sensor_type_is_string_enum(self) -> None:
        """Verify SensorType members are string values."""
        for member in SensorType:
            assert isinstance(member.value, str)


class TestIoTEvent:
    """Tests for the IoTEvent Pydantic model."""

    def test_valid_event_creation(self, sample_event: dict) -> None:
        """Verify a valid event dict creates an IoTEvent successfully."""
        event = IoTEvent(**sample_event)
        assert event.event_id == "evt-001"
        assert event.sensor_type == SensorType.TEMPERATURE
        assert event.value == 75.5
        assert event.quality_score == 0.98
        assert event.is_anomaly is False

    def test_default_quality_score(self) -> None:
        """Verify quality_score defaults to 1.0 when omitted."""
        event = IoTEvent(
            event_id="evt-002",
            sensor_id="s-001",
            sensor_type=SensorType.VIBRATION,
            facility_id="PLANT-A",
            timestamp=datetime.utcnow(),
            value=2.5,
            unit="mm/s",
        )
        assert event.quality_score == 1.0
        assert event.is_anomaly is False
        assert event.metadata is None

    def test_invalid_sensor_type_rejected(self, sample_event: dict) -> None:
        """Verify an invalid sensor_type raises a validation error."""
        sample_event["sensor_type"] = "invalid_type"
        with pytest.raises(Exception):
            IoTEvent(**sample_event)

    def test_quality_score_bounds(self, sample_event: dict) -> None:
        """Verify quality_score must be between 0.0 and 1.0."""
        sample_event["quality_score"] = 1.5
        with pytest.raises(Exception):
            IoTEvent(**sample_event)

        sample_event["quality_score"] = -0.1
        with pytest.raises(Exception):
            IoTEvent(**sample_event)

    def test_missing_required_field_rejected(self) -> None:
        """Verify missing required fields raise validation errors."""
        with pytest.raises(Exception):
            IoTEvent(event_id="evt-003")


class TestGetSparkSchema:
    """Tests for the PySpark schema factory function."""

    def test_get_spark_schema_returns_struct_type(self) -> None:
        """Verify get_spark_schema returns a StructType with correct fields."""
        mock_struct_type = MagicMock()
        mock_struct_type.fields = [MagicMock() for _ in range(10)]

        with patch("src.ingestion.schema_registry.get_spark_schema") as mock_fn:
            mock_fn.return_value = mock_struct_type
            schema = mock_fn()
            assert len(schema.fields) == 10

    def test_schema_has_expected_field_names(self) -> None:
        """Verify the schema includes all IoT event fields."""
        expected_fields = {
            "event_id", "sensor_id", "sensor_type", "facility_id",
            "timestamp", "value", "unit", "quality_score", "is_anomaly", "metadata",
        }

        mock_fields = []
        for name in expected_fields:
            f = MagicMock()
            f.name = name
            mock_fields.append(f)

        mock_schema = MagicMock()
        mock_schema.fields = mock_fields

        with patch("src.ingestion.schema_registry.get_spark_schema", return_value=mock_schema):
            from src.ingestion.schema_registry import get_spark_schema
            schema = get_spark_schema()
            field_names = {f.name for f in schema.fields}
            assert field_names == expected_fields


class TestValidateEvent:
    """Tests for the event validation function."""

    def test_validate_valid_event(self, sample_event: dict) -> None:
        """Verify valid event returns an IoTEvent instance."""
        result = validate_event(sample_event)
        assert result is not None
        assert isinstance(result, IoTEvent)
        assert result.event_id == sample_event["event_id"]

    def test_validate_invalid_event_returns_none(self) -> None:
        """Verify invalid event returns None."""
        result = validate_event({"event_id": "bad", "unknown_field": 123})
        assert result is None

    def test_validate_empty_dict_returns_none(self) -> None:
        """Verify empty dict returns None."""
        result = validate_event({})
        assert result is None

    def test_validate_event_with_all_sensor_types(self, sample_event: dict) -> None:
        """Verify events for all sensor types validate correctly."""
        for sensor_type in SensorType:
            sample_event["sensor_type"] = sensor_type.value
            result = validate_event(sample_event)
            assert result is not None
            assert result.sensor_type == sensor_type
