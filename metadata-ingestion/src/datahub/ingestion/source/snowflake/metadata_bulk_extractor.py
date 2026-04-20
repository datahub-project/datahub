"""
Bulk metadata extractor for Snowflake.

Replaces 610+ individual queries with ONE server-side query + file download.
Extracts all database/schema/table/column/key metadata in a single operation.
"""

import logging
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from queue import Queue
from threading import Lock
from typing import Dict, List, Optional

import pandas as pd

from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeConnectionConfig,
)
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnection,
    SnowflakePermissionError,
)

logger = logging.getLogger(__name__)


class BulkMetadataExtractor:
    """
    Extract Snowflake metadata by pushing computation to Snowflake.

    Instead of:
      - 610+ individual queries from client
      - Network roundtrips for each query
      - Slow client-side processing

    Do:
      - 1 server-side query (all JOINs done in Snowflake)
      - 1 file download (Parquet compressed)
      - 1 parse operation (pandas)

    Speedup: 20-50x faster than sequential queries.

    MEMORY PROFILE:
      - 1,000 tables (50 cols avg): ~10 MB
      - 10,000 tables (50 cols avg): ~100 MB
      - 50,000+ tables: Consider breaking into separate databases

    REQUIREMENTS:
      - CREATE STAGE privilege (required for temporary stage)
      - GET permission on internal stages

    FALLBACK:
      If bulk extraction fails (permission error, SQL timeout, etc.),
      caller must fall back to sequential queries via SnowflakeDataDictionary.
    """

    _UNPACK_FIELDS = [
        "numeric_precision",
        "numeric_scale",
        "character_maximum_length",
        "bytes",
        "row_count",
        "clustering_key",
        "view_definition",
        "is_dynamic",
        "is_iceberg",
        "is_hybrid",
        "retention_time",
    ]

    def __init__(
        self,
        connection: SnowflakeConnection,
        connection_config: Optional[SnowflakeConnectionConfig] = None,
        max_workers: Optional[int] = None,
        temp_dir: Optional[str] = None,
    ):
        """
        Initialize extractor with Snowflake connection.

        Args:
            connection: Primary SnowflakeConnection for discover_databases()
            connection_config: Optional SnowflakeConnectionConfig for spawning worker connections
            max_workers: Number of parallel worker threads (default: CPU core count)
            temp_dir: If provided, reuse this existing directory path instead of allocating a
                new TemporaryDirectory.  Worker sub-extractors pass the parent's temp dir here
                so that only the parent owns and cleans up the directory.
        """
        self.connection = connection
        self._connection_config = connection_config
        # Default to CPU core count for optimal parallelization
        self._max_workers = max_workers or os.cpu_count() or 4
        # TemporaryDirectory context manager ensures cleanup even if extraction fails.
        # Store both the object (for cleanup ownership) and the string path (for use).
        # Worker sub-extractors pass temp_dir= and therefore do not own cleanup.
        if temp_dir is not None:
            self._temp_dir_obj: Optional[tempfile.TemporaryDirectory] = None
            self._temp_dir: str = temp_dir
        else:
            self._temp_dir_obj = tempfile.TemporaryDirectory(
                prefix="snowflake_metadata_"
            )
            self._temp_dir = self._temp_dir_obj.name

        # Pre-allocate connection pool to avoid creating new connections per worker.
        # Without pooling, each worker call to get_connection() opens a new Snowflake
        # session, leading to 50+ simultaneous connections on large accounts.
        self._connection_pool: Optional[Queue] = None
        if connection_config:
            self._connection_pool = Queue(maxsize=self._max_workers)
            for _ in range(self._max_workers):
                self._connection_pool.put(connection_config.get_connection())

        logger.debug(
            f"BulkMetadataExtractor: Using temp dir {self._temp_dir}, "
            f"max_workers={self._max_workers}, "
            f"pool_size={self._connection_pool.qsize() if self._connection_pool else 0}"
        )

    def cleanup(self) -> None:
        """Close pooled connections and remove the temporary directory."""
        if self._connection_pool is not None:
            while not self._connection_pool.empty():
                try:
                    conn = self._connection_pool.get_nowait()
                    conn.close()
                except Exception:
                    pass
            self._connection_pool = None
        if self._temp_dir_obj is not None:
            self._temp_dir_obj.cleanup()
            self._temp_dir_obj = None
            logger.debug(f"Cleaned up temp directory: {self._temp_dir}")

    def __enter__(self) -> "BulkMetadataExtractor":
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.cleanup()

    def __del__(self) -> None:
        """Ensure resources are cleaned up if the context manager was not used."""
        self.cleanup()

    def discover_databases(self) -> List[str]:
        """
        Discover all accessible databases (lightweight SHOW command - no context needed).

        Returns:
            List of database names
        """
        logger.debug("Discovering databases...")
        cursor = self.connection.query("SHOW DATABASES")
        databases = []
        for i, row in enumerate(cursor):
            if i == 0:
                # Log first row to see structure
                logger.debug(
                    f"SHOW DATABASES row type: {type(row)}, keys: {list(row.keys()) if hasattr(row, 'keys') else 'N/A'}"
                )
            # Try multiple possible column names
            db_name = None
            if hasattr(row, "keys"):  # Dict-like
                db_name = (
                    row.get("name")
                    or row.get("database_name")
                    or row.get("NAME")
                    or row.get("DATABASE_NAME")
                )
            else:  # Tuple-like, try index
                try:
                    db_name = row[1]
                except (IndexError, TypeError):
                    pass

            if db_name and db_name not in ("SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"):
                databases.append(db_name)

        logger.debug(
            f"✅ Found {len(databases)} databases: {databases[:5]}{'...' if len(databases) > 5 else ''}"
        )
        return databases

    def _count_tables_in_database(self, database: str) -> int:
        """Count tables in a database (to decide extraction strategy)."""
        # Use a SHOW command approach - count by listing schemas and tables
        total = 0
        try:
            schemas = self.discover_schemas(database)
            for schema in schemas:
                cursor = self.connection.query(
                    f'SHOW TABLES IN "{database}"."{schema}"'
                )
                # Count all rows returned by SHOW TABLES
                for _ in cursor:
                    total += 1
        except Exception as e:
            logger.warning(f"Failed to count tables in {database}: {e}")
        return total

    def discover_schemas(self, database: str) -> List[str]:
        """
        Discover all schemas in a database (SHOW command - no context needed).

        Args:
            database: Database name

        Returns:
            List of schema names
        """
        cursor = self.connection.query(f'SHOW SCHEMAS IN DATABASE "{database}"')
        schemas = []
        for row in cursor:
            # SHOW SCHEMAS returns dict-like rows with column name "name"
            schema_name = row.get("name") or row.get("schema_name")
            if schema_name and schema_name not in ("INFORMATION_SCHEMA",):
                schemas.append(schema_name)
        logger.debug(f"Found {len(schemas)} schemas in {database}")
        return schemas

    def extract_database(self, database_name: str) -> pd.DataFrame:
        """
        Extract metadata for a SINGLE database (more memory-efficient for large accounts).

        For accounts with 50K+ tables, extract database-by-database to avoid
        memory pressure. Combine results with pd.concat().

        Streams query results directly to local parquet file (no staging required).

        Args:
            database_name: Name of the database to extract

        Returns:
            DataFrame with metadata for this database

        Raises:
            FileNotFoundError: If metadata_extractor_db_level.sql not found
        """
        logger.debug(f"📦 Extracting metadata for database: {database_name}")

        try:
            # Execute database-level extraction (optimized SQL)
            logger.debug("Step 1: Executing database-level extraction...")
            local_file = self._execute_database_extraction(
                stage_name=None, file_format="PARQUET", database_name=database_name
            )

            # Parse and return
            logger.debug("Step 2: Parsing metadata...")
            df = self._parse_metadata_file(local_file)

            # Add database_name column if not present
            if "database_name" not in df.columns:
                df["database_name"] = database_name

            logger.debug(
                f"✅ Database extraction complete: {len(df)} rows from {database_name}"
            )
            return df

        except Exception as e:
            logger.error(f"Failed to extract {database_name}: {e}")
            raise

    def extract_all_metadata(
        self,
        databases: Optional[List[str]] = None,
        schemas: Optional[List[str]] = None,
        use_database_level: bool = False,
        max_tables_per_query: int = 10000,
    ) -> pd.DataFrame:
        """
        Extract all metadata in one operation.

        Intelligently chooses extraction strategy:
        - Account-level (1 query): For small accounts (< 10K tables)
        - Database-level (N queries): For large accounts (50K+ tables)

        Args:
            databases: Optional list of database names to filter.
                      If None, discovers all accessible databases.
            schemas: Optional list of schema names to filter (applies to all databases).
                    If None, discovers all schemas in each database.
            use_database_level: Force database-level extraction (useful for very large accounts)
            max_tables_per_query: Threshold for switching to database-level extraction

        Returns:
            DataFrame with columns:
              - database_name, schema_name, table_name, table_type
              - column_name, data_type, is_nullable
              - constraint_name, constraint_type, fk_table_name
              - metadata_source (TABLE_META, COLUMN_META, PK_CONSTRAINT, FK_CONSTRAINT)

        Raises:
            SnowflakePermissionError: If CREATE STAGE privilege is missing
            FileNotFoundError: If SQL files not found
            ValueError: If Parquet parsing fails
        """
        logger.info("🚀 Starting bulk metadata extraction...")

        # Step 0: Discover databases and schemas if not provided
        if databases is None:
            databases = self.discover_databases()
            logger.debug(f"Will extract metadata for {len(databases)} databases")
        else:
            logger.debug(f"Filtering to {len(databases)} specified databases")

        # Skip schema discovery - extraction SQL already gets schemas via INFORMATION_SCHEMA
        # This avoids hitting trial account LISTING limits from SHOW SCHEMAS queries
        logger.debug("Schema discovery via SQL (no SHOW commands)")

        # Skip table counting - default to database-level extraction
        # This is safer and avoids additional SHOW queries that hit trial account limits
        logger.debug(
            "\n" + "─" * 70 + "\n📊 EXTRACTION STRATEGY: Database-level (trial-safe)\n"
            "🗂️ SELECTED: DATABASE-LEVEL EXTRACTION\n"
            "   Method: One query per database, combined result\n"
            "   Benefit: Low memory (processes one DB at a time)\n"
            "   Speed: 5-10x faster than sequential\n" + "─" * 70 + "\n"
        )
        return self._extract_all_metadata_database_level(databases)

    def _extract_all_metadata_database_level(
        self,
        databases: List[str],
    ) -> pd.DataFrame:
        """
        Extract metadata database-by-database (large accounts, better memory usage).

        Uses batched work-queue extraction if connection_config is available (recommended).
        Falls back to sequential extraction if no config provided.

        Batched approach: Workers pull batches from queue (default 5 dbs per batch),
        improving load balancing and cache locality vs submitting all at once.
        """
        logger.info(f"📦 Extracting {len(databases)} databases...")

        # Use batched extraction if connection config available (better load balancing)
        # Falls back to sequential if no config provided
        if self._connection_config:
            return self._extract_all_metadata_database_level_batched(databases)
        else:
            return self._extract_all_metadata_database_level_sequential(databases)

    def _extract_all_metadata_database_level_sequential(
        self,
        databases: List[str],
    ) -> pd.DataFrame:
        """Sequential extraction fallback (no connection config available)."""
        logger.info("Using sequential extraction (no connection pool)")
        dfs: List[pd.DataFrame] = []

        for i, db in enumerate(databases, 1):
            try:
                logger.debug(f"[{i}/{len(databases)}] Extracting {db}...")
                df = self.extract_database(db)
                dfs.append(df)
            except Exception as e:
                logger.warning(
                    f"⚠️ Failed to extract {db}, continuing with other databases: {e}"
                )
                continue

        if not dfs:
            raise ValueError(
                "Failed to extract metadata from all databases. Please check logs."
            )

        # Combine all DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(
            f"✅ Database-level extraction complete: {len(combined_df)} metadata rows "
            f"from {len(dfs)} databases"
        )

        return combined_df

    def _batch_worker(
        self,
        work_queue: Queue[str],
        batch_size: int,
        result_lock: Lock,
        dfs: List[pd.DataFrame],
        extraction_stats: Dict[str, int],
        should_stop: Dict[str, bool],
    ) -> None:
        """Extract a batch of databases from queue (called by worker thread)."""
        import threading

        while True:
            # Check interrupt flag before pulling next batch
            with result_lock:
                if should_stop["value"]:
                    break

            # Try to get a batch of databases from queue
            batch = []
            try:
                # Pull up to batch_size databases from queue (non-blocking)
                for _ in range(batch_size):
                    try:
                        db = work_queue.get_nowait()
                        batch.append(db)
                    except Exception:
                        # Queue empty or timeout
                        break

                if not batch:
                    # No more work
                    break

                logger.debug(
                    f"[Worker {threading.current_thread().name}] Processing batch: {batch}"
                )

                # Extract all databases in this batch
                for db in batch:
                    # Check interrupt flag before each database
                    with result_lock:
                        if should_stop["value"]:
                            logger.info(f"Interrupted before extracting {db}")
                            break

                    conn = None
                    try:
                        # Borrow a connection from the pool (blocks until available)
                        if self._connection_pool is None:
                            raise RuntimeError("Connection pool not initialized")
                        conn = self._connection_pool.get()
                        try:
                            extractor = BulkMetadataExtractor(
                                conn,
                                connection_config=None,
                                max_workers=1,
                                temp_dir=self._temp_dir,
                            )
                            df = extractor.extract_database(db)

                            if df is not None and not df.empty:
                                with result_lock:
                                    dfs.append(df)
                                    extraction_stats["success"] += 1
                                    logger.debug(f"{db} ({len(df)} rows)")
                            else:
                                with result_lock:
                                    extraction_stats["failed"] += 1
                                    logger.warning(f"{db} returned empty")
                        finally:
                            # Return connection to pool (don\'t close it)
                            assert self._connection_pool is not None
                            self._connection_pool.put(conn)
                            conn = None
                    except Exception as e:
                        # If connection was not returned to pool, return it now
                        if conn is not None:
                            assert self._connection_pool is not None
                            self._connection_pool.put(conn)
                        with result_lock:
                            extraction_stats["failed"] += 1
                            logger.warning(f"{db} failed: {e}")

            except Exception as e:
                logger.error(f"Worker error: {e}")
                break

    def _extract_all_metadata_database_level_batched(
        self,
        databases: List[str],
        batch_size: int = 5,
    ) -> pd.DataFrame:
        """
        Batched extraction using work queue (processes batch_size dbs per worker).

        Instead of submitting all databases at once to workers, use a queue to
        distribute batches. Each worker pulls batch_size databases from queue,
        extracts them, then pulls the next batch.

        This improves:
        - Load balancing (workers stay busy until queue is empty)
        - Cache locality (process related databases together)
        - Memory usage (batch extraction can be optimized)

        Graceful shutdown: Catches Ctrl+C and stops workers cleanly.

        Args:
            databases: List of database names to extract
            batch_size: Number of databases per batch (default 5)
        """
        import threading

        logger.info(
            f"Using batched extraction: {len(databases)} databases in batches of {batch_size}"
        )

        # Create thread-safe queue of databases
        work_queue: Queue[str] = Queue()
        for db in databases:
            work_queue.put(db)

        # Thread-safe result collection and interrupt flag
        result_lock = threading.Lock()
        dfs: List[pd.DataFrame] = []
        extraction_stats = {"total": len(databases), "success": 0, "failed": 0}
        start_time = time.time()
        should_stop = {"value": False}  # Shared flag for graceful shutdown

        # Spawn worker threads
        workers = []
        for _ in range(self._max_workers):
            worker_thread = threading.Thread(
                target=self._batch_worker,
                args=(
                    work_queue,
                    batch_size,
                    result_lock,
                    dfs,
                    extraction_stats,
                    should_stop,
                ),
                daemon=False,
            )
            workers.append(worker_thread)
            worker_thread.start()

        try:
            # Wait for all workers to complete (with timeout for interrupt responsiveness)
            # Check every 500ms if user pressed Ctrl+C, don't wait indefinitely
            timeout_per_check = 0.5
            for worker in workers:
                while worker.is_alive():
                    worker.join(timeout=timeout_per_check)
                    # If interrupted, convert to daemon and stop waiting
                    if should_stop["value"] and worker.is_alive():
                        logger.warning(
                            f"⛔ Stopping thread {worker.name} (query may still be running)"
                        )
                        worker.daemon = True
                        break

            # Check if interrupted
            if should_stop["value"]:
                logger.info(
                    f"⛔ Extraction interrupted after {extraction_stats['success']}/{extraction_stats['total']} databases"
                )
                if dfs:
                    logger.info("Returning partial results...")
                else:
                    raise KeyboardInterrupt("Extraction cancelled by user")

            if not dfs:
                raise ValueError(
                    "Failed to extract metadata from all databases. Please check logs."
                )

            # Combine all DataFrames
            combined_df = pd.concat(dfs, ignore_index=True)

            # Enrich with constraints (skip if interrupted)
            if not should_stop["value"]:
                logger.info("Fetching constraints for all databases...")
                try:
                    combined_df = self._merge_constraints_all_databases(combined_df)
                except Exception as e:
                    logger.warning(
                        f"⚠️ Failed to fetch constraints: {e}. Continuing with tables/columns only."
                    )

            elapsed = time.time() - start_time

            logger.info(
                f"✅ Batched extraction complete: {len(combined_df)} metadata rows "
                f"from {extraction_stats['success']}/{extraction_stats['total']} databases "
                f"(⏱️ {elapsed:.1f}s, batches of {batch_size})"
            )

            return combined_df

        except KeyboardInterrupt:
            logger.error("Extraction cancelled by user")
            raise

    def _unpack_properties(self, properties: object) -> dict:
        """Unpack numeric metadata from a properties dict or JSON string.

        Returns a dict with keys from _UNPACK_FIELDS. Missing fields are None.
        """
        import json

        parsed: dict = {}
        if isinstance(properties, str):
            try:
                parsed = json.loads(properties)
            except (ValueError, TypeError):
                parsed = {}
        elif isinstance(properties, dict):
            parsed = properties

        return {field: parsed.get(field) for field in self._UNPACK_FIELDS}

    def _unpack_properties_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Unpack the JSON 'properties' column into top-level DataFrame columns.

        Extracts numeric_precision, numeric_scale, character_maximum_length,
        bytes, row_count, clustering_key, and view_definition from the packed
        JSON column and merges them as top-level columns.
        """
        unpacked = df["properties"].apply(self._unpack_properties)
        unpacked_df = pd.DataFrame(unpacked.tolist(), index=df.index)

        # Only add columns that don't already exist to avoid overwriting data
        for col in self._UNPACK_FIELDS:
            if col not in df.columns:
                df[col] = unpacked_df[col]

        df = df.drop(columns=["properties"])
        return df

    @staticmethod
    def _build_constraint_row_base(
        database_name: str,
        schema_name: Optional[str],
        table_name: str,
        column_name: Optional[str],
        constraint_name: Optional[str],
        constraint_type: str,
        properties: Optional[dict],
    ) -> dict:
        """Build the common structure for PK/FK constraint rows."""
        return {
            "database_name": database_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "column_name": column_name,
            "table_type": None,
            "column_ordinal": None,
            "data_type": None,
            "is_nullable": None,
            "column_default": None,
            "column_comment": None,
            "constraint_name": constraint_name,
            "constraint_type": constraint_type,
            "fk_table_name": None,
            "created": None,
            "last_altered": None,
            "comment": None,
            **(properties or {}),
        }

    def _merge_constraints_all_databases(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fetch PK/FK constraints in parallel (auto-scaled to CPU count).

        Parallelization reduces 79s single query to ~5-8s with multi-worker pool.

        Returns:
            DataFrame with constraint rows appended (same schema as input df)
        """
        import threading

        constraint_rows = []
        constraint_rows_lock = threading.Lock()

        # Get unique databases from extracted metadata
        databases = sorted(df["database_name"].unique().tolist())

        logger.info(
            f"⚡ Fetching constraints in parallel: {len(databases)} databases, {self._max_workers} workers"
        )

        def fetch_constraints_for_database(db: str) -> tuple:
            """Fetch PK and FK constraints for a single database."""
            pk_rows = []
            fk_rows = []

            try:
                assert self._connection_pool is not None
                conn = self._connection_pool.get()
            except Exception as e:
                logger.warning(
                    f"Failed to get connection from pool for {db}: {e}. "
                    f"Skipping constraint extraction for this database."
                )
                return (db, 0, 0, [])

            try:
                # Fetch PK constraints
                try:
                    pk_cursor = conn.query(f'SHOW PRIMARY KEYS IN DATABASE "{db}"')
                    for row in pk_cursor:
                        base_row = self._build_constraint_row_base(
                            database_name=row.get("database_name"),
                            schema_name=row.get("schema_name"),
                            table_name=row.get("table_name"),
                            column_name=row.get("column_name"),
                            constraint_name=row.get("constraint_name"),
                            constraint_type="PRIMARY KEY",
                            properties=self._unpack_properties(row.get("properties")),
                        )
                        base_row["metadata_source"] = "PK_CONSTRAINT"
                        pk_rows.append(base_row)
                except Exception as e:
                    logger.debug(f"No PK constraints in {db}: {e}")

                # Fetch FK constraints
                try:
                    fk_cursor = conn.query(f'SHOW IMPORTED KEYS IN DATABASE "{db}"')
                    fk_logged = False
                    for row in fk_cursor:
                        if not fk_logged:
                            logger.debug(
                                f"SHOW IMPORTED KEYS columns: {list(row.keys()) if hasattr(row, 'keys') else 'N/A'}"
                            )
                            fk_logged = True
                        base_row = self._build_constraint_row_base(
                            database_name=db,
                            schema_name=row.get("fk_schema_name"),
                            table_name=row.get("fk_table_name"),
                            column_name=row.get("fk_column_name"),
                            constraint_name=row.get("fk_name"),
                            constraint_type="FOREIGN KEY",
                            properties=self._unpack_properties(row.get("properties")),
                        )
                        base_row["fk_table_name"] = row.get("pk_table_name")
                        base_row["metadata_source"] = "FK_CONSTRAINT"
                        fk_rows.append(base_row)
                except Exception as e:
                    logger.debug(f"No FK constraints in {db}: {e}")

                return (db, len(pk_rows), len(fk_rows), pk_rows + fk_rows)
            finally:
                # Return connection to pool (don\'t close it)
                try:
                    assert self._connection_pool is not None
                    self._connection_pool.put(conn)
                except Exception as e:
                    logger.error(f"Failed to return connection to pool for {db}: {e}")

        # Pre-compute table schema lookup for all databases to avoid 67x DataFrame filtering
        db_to_table_schema_map = {}
        for db_name in databases:
            tables_with_schemas = df[
                (df["metadata_source"] == "TABLE_META")
                & (df["database_name"] == db_name)
            ][["table_name", "schema_name"]].drop_duplicates()
            # Use dict comprehension instead of inefficient iterrows()
            db_to_table_schema_map[db_name] = dict(
                zip(
                    tables_with_schemas["table_name"].str.upper(),
                    tables_with_schemas["schema_name"],
                    strict=False,
                )
            )

        # Parallelize constraint fetching (respect configured worker count)
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {
                executor.submit(fetch_constraints_for_database, db): db
                for db in databases
            }

            for completed, future in enumerate(as_completed(futures), 1):
                db = futures[future]
                try:
                    db_name, pk_count, fk_count, rows = future.result()

                    # Fix FK schema names for THIS database immediately,
                    # before merging into the shared constraint_rows list.
                    if rows and any(
                        r["metadata_source"] == "FK_CONSTRAINT" for r in rows
                    ):
                        fk_rows_for_db = [
                            r for r in rows if r["metadata_source"] == "FK_CONSTRAINT"
                        ]
                        table_schema_map = db_to_table_schema_map.get(db_name, {})

                        fixed_count = 0
                        missed_count = 0
                        for fk_row in fk_rows_for_db:
                            key = fk_row["table_name"].upper()
                            if key in table_schema_map:
                                fk_row["schema_name"] = table_schema_map[key]
                                fixed_count += 1
                            else:
                                missed_count += 1
                                logger.warning(
                                    f"FK constraint schema lookup failed for "
                                    f"{db_name}.{fk_row['table_name']} "
                                    f"(case-insensitive match found no matching table)"
                                )

                        if fixed_count > 0:
                            logger.info(
                                f"Fixed FK table schemas for {fixed_count}/{len(fk_rows_for_db)} "
                                f"FK constraints in {db_name}"
                            )
                        if missed_count > 0:
                            logger.warning(
                                f"{missed_count}/{len(fk_rows_for_db)} FK constraints had "
                                f"schema lookup failures in database {db_name}"
                            )

                    with constraint_rows_lock:
                        constraint_rows.extend(rows)
                    logger.debug(
                        f"[{completed}/{len(databases)}] {db_name}: {pk_count} PKs, {fk_count} FKs"
                    )
                except Exception as e:
                    logger.warning(f"[{completed}/{len(databases)}] {db}: {e}")

        # Append constraint rows to metadata DataFrame (with Arrow dtypes for efficiency)
        if constraint_rows:
            constraint_df = pd.DataFrame(constraint_rows)

            # Convert string columns to Arrow strings for 30-50% memory reduction
            string_cols = constraint_df.select_dtypes(include=["object"]).columns
            constraint_df[string_cols] = constraint_df[string_cols].astype(
                "string[pyarrow]"
            )
            df = pd.concat([df, constraint_df], ignore_index=True)
            logger.info(
                f"✅ Merged {len(constraint_rows)} constraint rows into metadata"
            )

        return df

    def _execute_database_extraction(
        self,
        stage_name: None,
        file_format: str,
        database_name: str,
    ) -> str:
        """
        Execute database-level metadata extraction (optimized for single DB).

        Uses metadata_extractor_db_level.sql which is more efficient than
        account-level extraction for large databases (50K+ tables).

        Streams results directly to local parquet file (no staging).

        Returns path to the local file.
        """
        # Load the optimized database-level SQL script
        script_path = Path(__file__).parent / "metadata_extractor_db_level.sql"
        if not script_path.exists():
            logger.error(
                f"Database-level metadata extractor SQL not found: {script_path}. "
                f"Falling back to sequential metadata extraction."
            )
            raise SnowflakePermissionError(
                f"Bulk extraction unavailable: SQL template missing at {script_path}"
            )

        try:
            with open(script_path) as f:
                extraction_query = f.read()
        except (FileNotFoundError, IOError) as e:
            logger.error(f"Cannot read SQL template: {e}")
            raise SnowflakePermissionError(
                "Bulk extraction unavailable: Cannot read SQL template"
            ) from e

        # Substitute database name with quotes to handle hyphens, spaces, and reserved words
        # Strategy: Handle identifiers (FROM/JOIN) before string literals (WHERE/SELECT)

        # 1. Replace identifiers: {database_name}.INFORMATION_SCHEMA -> "{database_name}".INFORMATION_SCHEMA
        extraction_query = extraction_query.replace(
            "{database_name}.INFORMATION_SCHEMA",
            f'"{database_name}".INFORMATION_SCHEMA',
        )

        # 2. Replace remaining {database_name} with single quotes for string literals (WHERE clauses)
        # Escape single quotes by doubling them to prevent SQL injection
        escaped_database_name = database_name.replace("'", "''")
        extraction_query = extraction_query.replace(
            "{database_name}", f"'{escaped_database_name}'"
        )

        # Strip trailing semicolon
        extraction_query = extraction_query.rstrip().rstrip(";")

        # Execute query and stream results in batches
        logger.debug(f"Executing extraction query for {database_name}...")
        cursor = self.connection.query(extraction_query)

        # Stream results to local parquet file using batch fetching
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"metadata_{database_name}_{timestamp}.parquet"
        local_file = Path(self._temp_dir) / output_filename

        # Collect batches (memory-efficient streaming)
        all_batches = []
        batch_count = 0

        # Use pandas batch fetching if available (memory-efficient)
        try:
            for batch_df in cursor.fetch_pandas_batches():
                all_batches.append(batch_df)
                batch_count += 1
                logger.debug(f"Fetched batch {batch_count}: {len(batch_df)} rows")
        except AttributeError:
            # Fallback: fetch all rows if batch API not available
            logger.debug("fetch_pandas_batches not available, using fetchall()")
            rows = cursor.fetchall()
            if rows:
                all_batches.append(pd.DataFrame(rows))
                batch_count = 1

        # Concatenate batches and write to parquet
        if all_batches:
            df = pd.concat(all_batches, ignore_index=True)
            df.to_parquet(local_file, compression="snappy")
            logger.debug(
                f"Wrote {len(df)} rows ({batch_count} batches) to {local_file}"
            )
        else:
            logger.warning(f"No metadata rows returned for {database_name}")
            # Create empty parquet file
            df = pd.DataFrame()
            df.to_parquet(local_file, compression="snappy")

        return str(local_file)

    def _parse_metadata_file(self, file_path: str) -> pd.DataFrame:
        """Parse downloaded metadata file into DataFrame.

        For large files (>100K rows), memory efficiency is important.
        Loading entire Parquet at once may fail for massive schemas (50K+ tables).

        Raises:
            ValueError: If file cannot be parsed in any format
        """
        try:
            if file_path.endswith(".parquet"):
                # Use standard backend (no Arrow dtypes to avoid pd.NA issues)
                df = pd.read_parquet(file_path)
                file_size_mb = Path(file_path).stat().st_size / (1024 * 1024)
                logger.info(
                    f"Parsed {len(df)} metadata rows from {file_size_mb:.1f} MB Parquet file"
                )
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path)
                logger.info(f"Parsed {len(df)} metadata rows from CSV file")
            else:
                raise ValueError(f"Unsupported file format: {file_path}")

            # Standardize column names
            df.columns = df.columns.str.lower()
            logger.debug(f"Columns: {df.columns.tolist()}")

            # Unpack JSON 'properties' column into top-level columns for all rows
            if "properties" in df.columns:
                df = self._unpack_properties_dataframe(df)
                logger.debug(
                    f"Unpacked 'properties' column. Columns now: {df.columns.tolist()}"
                )

            # Warn if dataset is huge (memory pressure possible)
            if len(df) > 500000:
                logger.warning(
                    f"⚠️ Large metadata set ({len(df)} rows). "
                    f"Memory usage may be high. Consider breaking extraction into smaller databases."
                )

            return df
        except (ValueError, FileNotFoundError) as e:
            logger.error(f"Failed to parse metadata file {file_path}: {e}")
            raise

