"""Apache Iceberg table utilities.

Provides helpers for time-travel queries, schema evolution,
snapshot management, and table maintenance operations.
"""

from __future__ import annotations

from typing import Any, Optional

import structlog
from pyspark.sql import DataFrame, SparkSession

logger = structlog.get_logger(__name__)

CATALOG_NAME = "lakehouse"


class IcebergManager:
    """Manages Apache Iceberg tables for the data lakehouse.

    Provides utilities for creating, querying, and maintaining
    Iceberg tables with time-travel and schema evolution support.

    Attributes:
        spark: Active SparkSession with Iceberg catalog configured.
        catalog: Iceberg catalog name.
    """

    def __init__(self, spark: SparkSession, catalog: str = CATALOG_NAME) -> None:
        """Initialize the Iceberg table manager.

        Args:
            spark: SparkSession with Iceberg extensions enabled.
            catalog: Name of the Iceberg catalog.
        """
        self.spark = spark
        self.catalog = catalog
        logger.info("iceberg_manager_initialized", catalog=catalog)

    def create_table(
        self,
        table_name: str,
        schema_df: DataFrame,
        partition_columns: list[str] | None = None,
        properties: dict[str, str] | None = None,
    ) -> None:
        """Create an Iceberg table from a DataFrame schema.

        Args:
            table_name: Fully qualified table name (database.table).
            schema_df: DataFrame whose schema to use for table creation.
            partition_columns: Columns to partition by.
            properties: Additional Iceberg table properties.
        """
        full_name = f"{self.catalog}.{table_name}"
        writer = schema_df.writeTo(full_name).using("iceberg")

        if partition_columns:
            for col in partition_columns:
                writer = writer.partitionedBy(col)

        if properties:
            for key, value in properties.items():
                writer = writer.tableProperty(key, value)

        writer.createOrReplace()
        logger.info("iceberg_table_created", table=full_name, partitions=partition_columns)

    def time_travel(
        self,
        table_name: str,
        snapshot_id: Optional[int] = None,
        as_of_timestamp: Optional[str] = None,
    ) -> DataFrame:
        """Query an Iceberg table at a specific point in time.

        Args:
            table_name: Fully qualified table name.
            snapshot_id: Specific snapshot ID to read.
            as_of_timestamp: ISO timestamp for point-in-time query.

        Returns:
            DataFrame at the specified point in time.
        """
        full_name = f"{self.catalog}.{table_name}"
        reader = self.spark.read.format("iceberg")

        if snapshot_id is not None:
            reader = reader.option("snapshot-id", str(snapshot_id))
        elif as_of_timestamp is not None:
            reader = reader.option("as-of-timestamp", as_of_timestamp)

        df = reader.load(full_name)
        logger.info(
            "time_travel_query",
            table=full_name,
            snapshot_id=snapshot_id,
            as_of=as_of_timestamp,
        )
        return df

    def list_snapshots(self, table_name: str) -> list[dict[str, Any]]:
        """List all snapshots for an Iceberg table.

        Args:
            table_name: Fully qualified table name.

        Returns:
            List of snapshot metadata dictionaries.
        """
        full_name = f"{self.catalog}.{table_name}"
        snapshots_df = self.spark.sql(
            f"SELECT * FROM {full_name}.snapshots ORDER BY committed_at DESC"
        )
        snapshots = [row.asDict() for row in snapshots_df.collect()]
        logger.info("snapshots_listed", table=full_name, count=len(snapshots))
        return snapshots

    def get_table_history(self, table_name: str) -> list[dict[str, Any]]:
        """Get the change history of an Iceberg table.

        Args:
            table_name: Fully qualified table name.

        Returns:
            List of history entries with timestamps and snapshot IDs.
        """
        full_name = f"{self.catalog}.{table_name}"
        history_df = self.spark.sql(f"SELECT * FROM {full_name}.history")
        return [row.asDict() for row in history_df.collect()]

    def evolve_schema(
        self,
        table_name: str,
        add_columns: dict[str, str] | None = None,
        rename_columns: dict[str, str] | None = None,
    ) -> None:
        """Evolve the schema of an Iceberg table.

        Args:
            table_name: Fully qualified table name.
            add_columns: New columns to add {name: type}.
            rename_columns: Columns to rename {old_name: new_name}.
        """
        full_name = f"{self.catalog}.{table_name}"

        if add_columns:
            for col_name, col_type in add_columns.items():
                self.spark.sql(
                    f"ALTER TABLE {full_name} ADD COLUMN {col_name} {col_type}"
                )
                logger.info("column_added", table=full_name, column=col_name, type=col_type)

        if rename_columns:
            for old_name, new_name in rename_columns.items():
                self.spark.sql(
                    f"ALTER TABLE {full_name} RENAME COLUMN {old_name} TO {new_name}"
                )
                logger.info("column_renamed", table=full_name, old=old_name, new=new_name)

    def expire_snapshots(self, table_name: str, older_than: str) -> None:
        """Expire old snapshots to reclaim storage.

        Args:
            table_name: Fully qualified table name.
            older_than: ISO timestamp; snapshots older than this will expire.
        """
        full_name = f"{self.catalog}.{table_name}"
        self.spark.sql(
            f"CALL {self.catalog}.system.expire_snapshots("
            f"table => '{full_name}', older_than => TIMESTAMP '{older_than}')"
        )
        logger.info("snapshots_expired", table=full_name, older_than=older_than)

    def compact_data_files(self, table_name: str) -> None:
        """Rewrite small data files into larger ones for better performance.

        Args:
            table_name: Fully qualified table name.
        """
        full_name = f"{self.catalog}.{table_name}"
        self.spark.sql(
            f"CALL {self.catalog}.system.rewrite_data_files(table => '{full_name}')"
        )
        logger.info("data_files_compacted", table=full_name)
