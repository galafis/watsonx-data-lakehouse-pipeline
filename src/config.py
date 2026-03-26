"""Centralized configuration management for the lakehouse pipeline.

Loads settings from environment variables and config/settings.yaml,
providing typed access to all pipeline configuration.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import structlog
import yaml
from pydantic import Field
from pydantic_settings import BaseSettings

logger = structlog.get_logger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "settings.yaml"


class WatsonxSettings(BaseSettings):
    """IBM Watsonx credentials and configuration."""

    api_key: str = Field(default="", alias="WATSONX_API_KEY")
    project_id: str = Field(default="", alias="WATSONX_PROJECT_ID")
    url: str = Field(default="https://us-south.ml.cloud.ibm.com", alias="WATSONX_URL")


class KafkaSettings(BaseSettings):
    """Kafka connection settings."""

    bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    topic_iot: str = Field(default="iot-sensor-events", alias="KAFKA_TOPIC_IOT")
    consumer_group: str = Field(default="lakehouse-pipeline", alias="KAFKA_CONSUMER_GROUP")


class MinIOSettings(BaseSettings):
    """MinIO object storage settings."""

    endpoint: str = Field(default="localhost:9000", alias="MINIO_ENDPOINT")
    access_key: str = Field(default="minioadmin", alias="MINIO_ACCESS_KEY")
    secret_key: str = Field(default="minioadmin", alias="MINIO_SECRET_KEY")
    bucket: str = Field(default="lakehouse", alias="MINIO_BUCKET")
    secure: bool = Field(default=False, alias="MINIO_SECURE")


class SparkSettings(BaseSettings):
    """Apache Spark settings."""

    master: str = Field(default="local[*]", alias="SPARK_MASTER")
    app_name: str = Field(default="watsonx-lakehouse-pipeline", alias="SPARK_APP_NAME")
    warehouse_dir: str = Field(
        default="s3a://lakehouse/warehouse", alias="SPARK_WAREHOUSE_DIR"
    )


class LakehouseSettings(BaseSettings):
    """Data lakehouse path settings."""

    bronze_path: str = Field(default="s3a://lakehouse/bronze", alias="BRONZE_PATH")
    silver_path: str = Field(default="s3a://lakehouse/silver", alias="SILVER_PATH")
    gold_path: str = Field(default="s3a://lakehouse/gold", alias="GOLD_PATH")


class Settings(BaseSettings):
    """Aggregated application settings."""

    watsonx: WatsonxSettings = Field(default_factory=WatsonxSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    minio: MinIOSettings = Field(default_factory=MinIOSettings)
    spark: SparkSettings = Field(default_factory=SparkSettings)
    lakehouse: LakehouseSettings = Field(default_factory=LakehouseSettings)

    api_host: str = Field(default="0.0.0.0", alias="API_HOST")
    api_port: int = Field(default=8080, alias="API_PORT")
    api_url: str = Field(default="http://localhost:8080", alias="API_URL")

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


def load_yaml_config() -> dict[str, Any]:
    """Load configuration from settings.yaml.

    Returns:
        Dictionary with YAML configuration values.
    """
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            config = yaml.safe_load(f)
            logger.info("yaml_config_loaded", path=str(CONFIG_PATH))
            return config or {}
    logger.warning("yaml_config_not_found", path=str(CONFIG_PATH))
    return {}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Get cached application settings singleton.

    Returns:
        Settings instance with all configuration loaded.
    """
    from dotenv import load_dotenv

    load_dotenv()
    settings = Settings()
    logger.info(
        "settings_loaded",
        spark_master=settings.spark.master,
        kafka_servers=settings.kafka.bootstrap_servers,
    )
    return settings


@lru_cache(maxsize=1)
def get_yaml_config() -> dict[str, Any]:
    """Get cached YAML configuration singleton.

    Returns:
        Dictionary with YAML configuration values.
    """
    return load_yaml_config()
