/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

/**
 * Tracks dataset schemas over time to detect schema mismatches that indicate RDD conversions.
 *
 * <p>When a DataFrame is converted to RDD and back (df.rdd → createDataFrame), Spark's logical plan
 * is lost. The LogicalRDD node reads from a dataset but imposes a new schema on top of it. This
 * class detects such mismatches by tracking both write and read schemas for each dataset.
 */
@Slf4j
public class SchemaHistoryTracker {

  private final Map<String, List<SchemaSnapshot>> datasetSchemas = new ConcurrentHashMap<>();
  private final Map<String, SchemaMismatch> detectedMismatches = new ConcurrentHashMap<>();
  private final long maxAge;
  private final int maxSnapshots;

  /** Schema snapshot containing schema information and metadata. */
  @Data
  @AllArgsConstructor
  public static class SchemaSnapshot {
    private final List<String> schema;
    private final long timestamp;
    private final OperationType operationType;
  }

  /** Type of operation that produced the schema snapshot. */
  public enum OperationType {
    WRITE,
    READ
  }

  /** Schema mismatch between write and read operations. */
  @Data
  @AllArgsConstructor
  public static class SchemaMismatch {
    private final String dataset;
    private final List<String> actualSchema;
    private final List<String> imposedSchema;

    /**
     * Creates a positional mapping from imposed schema to actual schema.
     *
     * @return map from imposed column name to actual column name
     */
    public Map<String, String> createPositionalMapping() {
      if (actualSchema.size() != imposedSchema.size()) {
        return Collections.emptyMap();
      }

      Map<String, String> mapping = new ConcurrentHashMap<>();
      for (int i = 0; i < actualSchema.size(); i++) {
        mapping.put(imposedSchema.get(i), actualSchema.get(i));
      }
      return mapping;
    }
  }

  /**
   * Create a new schema history tracker with default settings.
   *
   * <p>Default max age: 1 hour (3600000 ms) Default max snapshots per dataset: 10
   */
  public SchemaHistoryTracker() {
    this(3600000L, 10);
  }

  /**
   * Create a new schema history tracker with custom settings.
   *
   * @param maxAge maximum age of snapshots to retain (milliseconds)
   * @param maxSnapshots maximum number of snapshots to retain per dataset
   */
  public SchemaHistoryTracker(long maxAge, int maxSnapshots) {
    this.maxAge = maxAge;
    this.maxSnapshots = maxSnapshots;
  }

  /**
   * Record a dataset write operation with its schema.
   *
   * @param dataset dataset identifier (namespace/name)
   * @param schema list of column names
   */
  public void recordWrite(String dataset, List<String> schema) {
    recordOperation(dataset, schema, OperationType.WRITE);
  }

  /**
   * Record a dataset read operation with its schema.
   *
   * @param dataset dataset identifier (namespace/name)
   * @param schema list of column names
   */
  public void recordRead(String dataset, List<String> schema) {
    recordOperation(dataset, schema, OperationType.READ);
  }

  /**
   * Record a dataset operation (read or write).
   *
   * @param dataset dataset identifier
   * @param schema schema column names
   * @param operationType type of operation
   */
  private void recordOperation(String dataset, List<String> schema, OperationType operationType) {
    long timestamp = System.currentTimeMillis();
    SchemaSnapshot snapshot = new SchemaSnapshot(schema, timestamp, operationType);

    datasetSchemas.compute(
        dataset,
        (key, snapshots) -> {
          if (snapshots == null) {
            snapshots = new ArrayList<>();
          }

          // Add new snapshot
          snapshots.add(snapshot);

          // Clean up old snapshots
          snapshots = cleanup(snapshots, timestamp);

          return snapshots;
        });

    log.debug(
        "Recorded {} operation for dataset {} with schema {}", operationType, dataset, schema);
  }

  /**
   * Attempt to read schema from file metadata for a dataset.
   *
   * <p>This is useful for enriching INPUT datasets that don't have schema facets. The schema is
   * read from file metadata (Parquet, ORC, Delta, etc.) without loading the data.
   *
   * @param dataset dataset identifier (namespace/name format)
   * @return list of column names if successful, empty otherwise
   */
  public Optional<List<String>> tryReadSchemaFromFile(String dataset) {
    return readSchemaFromFile(dataset);
  }

  /**
   * Detect schema mismatch for a dataset.
   *
   * <p>A mismatch occurs when: 1. Multiple read operations exist for the same dataset 2. The
   * schemas differ between reads 3. Both schemas have the same number of columns
   *
   * <p>This indicates an RDD conversion where the logical plan was lost.
   *
   * <p>Strategy: Compare first READ (original schema from file) vs later READs (imposed schema from
   * RDD). If no prior read exists, fall back to reading file metadata.
   *
   * @param dataset dataset identifier
   * @return schema mismatch if detected, empty otherwise
   */
  public Optional<SchemaMismatch> detectMismatch(String dataset) {
    // Check if we've already detected a mismatch for this dataset
    SchemaMismatch cachedMismatch = detectedMismatches.get(dataset);
    if (cachedMismatch != null) {
      log.debug(
          "Using cached mismatch for dataset {}: {} -> {}",
          dataset,
          cachedMismatch.getActualSchema(),
          cachedMismatch.getImposedSchema());
      return Optional.of(cachedMismatch);
    }

    List<SchemaSnapshot> snapshots = datasetSchemas.get(dataset);

    if (snapshots == null || snapshots.isEmpty()) {
      return Optional.empty();
    }

    // Get all READ operations for this dataset
    List<SchemaSnapshot> readOperations =
        snapshots.stream()
            .filter(s -> s.operationType == OperationType.READ)
            .collect(Collectors.toList());

    if (readOperations.isEmpty()) {
      return Optional.empty();
    }

    // If we have multiple reads, compare first vs last
    if (readOperations.size() >= 2) {
      log.info(
          "Found {} READ operations for dataset {}, using optimized path",
          readOperations.size(),
          dataset);
      SchemaSnapshot firstRead = readOperations.get(0);
      SchemaSnapshot lastRead = readOperations.get(readOperations.size() - 1);

      List<String> actualSchema = firstRead.schema;
      List<String> imposedSchema = lastRead.schema;

      // Check for mismatch: different schemas, same column count
      if (!actualSchema.equals(imposedSchema) && actualSchema.size() == imposedSchema.size()) {
        log.info(
            "Detected schema mismatch for dataset {} (multiple reads): actual schema {}, imposed"
                + " schema {}",
            dataset,
            actualSchema,
            imposedSchema);
        SchemaMismatch mismatch = new SchemaMismatch(dataset, actualSchema, imposedSchema);
        detectedMismatches.put(dataset, mismatch);
        return Optional.of(mismatch);
      }

      return Optional.empty();
    }

    // Single read - try to find the actual schema from write or file metadata
    SchemaSnapshot singleRead = readOperations.get(0);
    List<String> imposedSchema = singleRead.schema;

    // Try to find write operation in memory
    Optional<SchemaSnapshot> firstWrite =
        snapshots.stream().filter(s -> s.operationType == OperationType.WRITE).findFirst();

    List<String> actualSchema = null;

    if (firstWrite.isPresent()) {
      // Found write operation in memory (same-application case)
      actualSchema = firstWrite.get().schema;
      log.debug("Using in-memory write schema for dataset {}: {}", dataset, actualSchema);
    } else {
      // No write operation in memory - try to read actual file schema (cross-application case)
      log.debug(
          "No write operation found in memory for dataset {}. Attempting to read file metadata...",
          dataset);
      Optional<List<String>> fileSchema = readSchemaFromFile(dataset);
      if (fileSchema.isPresent()) {
        actualSchema = fileSchema.get();
        log.info(
            "Successfully read file schema for dataset {} from metadata: {}",
            dataset,
            actualSchema);
      } else {
        log.debug(
            "Could not read file schema for dataset {}. Skipping mismatch detection.", dataset);
        return Optional.empty();
      }
    }

    // Check for mismatch: different schemas, same column count
    if (!actualSchema.equals(imposedSchema) && actualSchema.size() == imposedSchema.size()) {
      log.info(
          "Detected schema mismatch for dataset {}: actual schema {}, imposed schema {}",
          dataset,
          actualSchema,
          imposedSchema);
      SchemaMismatch mismatch = new SchemaMismatch(dataset, actualSchema, imposedSchema);
      detectedMismatches.put(dataset, mismatch);
      return Optional.of(mismatch);
    }

    return Optional.empty();
  }

  /**
   * Attempts to read the actual schema from RDD dependencies, file metadata, or table catalog.
   *
   * <p>This method tries multiple strategies to get the original schema:
   *
   * <ul>
   *   <li>FileScanRDD: Extract schema from Spark's FileScanRDD.readSchema (fastest, no I/O)
   *   <li>Hive/Spark Tables: Read from catalog metadata
   *   <li>Parquet/ORC: Read embedded schema from file footer
   *   <li>Delta Lake: Read schema from _delta_log metadata
   *   <li>Iceberg: Read schema from metadata files
   *   <li>CSV/TSV: Use Spark's schema inference with sampling
   * </ul>
   *
   * @param dataset dataset identifier (file path, table name, or URI)
   * @return list of column names if successful, empty otherwise
   */
  private Optional<List<String>> readSchemaFromFile(String dataset) {
    try {
      // Get active Spark session
      Optional<SparkSession> sparkSession = getActiveSparkSession();
      if (!sparkSession.isPresent()) {
        log.debug("No active Spark session available to read schema");
        return Optional.empty();
      }

      // Extract file path or table name from dataset identifier
      String filePath = extractFilePath(dataset);
      if (filePath == null || filePath.isEmpty()) {
        log.debug("Could not extract path from dataset: {}", dataset);
        return Optional.empty();
      }

      // Strategy 1: Try reading from Spark catalog (Hive/Spark tables)
      Optional<List<String>> schemaFromCatalog =
          readSchemaFromCatalog(sparkSession.get(), filePath);
      if (schemaFromCatalog.isPresent()) {
        log.debug(
            "Successfully read schema from catalog for {}: {}", filePath, schemaFromCatalog.get());
        return schemaFromCatalog;
      }

      // Strategy 2: Try reading from file metadata (format auto-detection)
      Optional<List<String>> schemaFromFile =
          readSchemaFromFileMetadata(sparkSession.get(), filePath);
      if (schemaFromFile.isPresent()) {
        log.debug(
            "Successfully read schema from file metadata for {}: {}",
            filePath,
            schemaFromFile.get());
        return schemaFromFile;
      }

    } catch (Exception e) {
      // This is expected in many cases (non-existent datasets, wrong format, etc.)
      log.debug("Could not read schema for dataset {}: {}", dataset, e.getMessage());
    }

    return Optional.empty();
  }

  /**
   * Attempts to read schema from Spark catalog (for Hive/Spark tables).
   *
   * @param spark Spark session
   * @param tableOrPath table name or path
   * @return list of column names if successful, empty otherwise
   */
  private Optional<List<String>> readSchemaFromCatalog(SparkSession spark, String tableOrPath) {
    try {
      // Try treating as a table reference (database.table or just table)
      // This works for Hive tables, Spark SQL tables, and catalog-registered tables
      String tableName = tableOrPath;

      // Remove leading slashes if present (not a table name)
      if (tableName.startsWith("/")) {
        return Optional.empty();
      }

      // Check if table exists in catalog
      if (!spark.catalog().tableExists(tableName)) {
        // Try default database
        String defaultDb = spark.catalog().currentDatabase();
        String qualifiedName = defaultDb + "." + tableName;
        if (!spark.catalog().tableExists(qualifiedName)) {
          return Optional.empty();
        }
        tableName = qualifiedName;
      }

      // Get table schema from catalog (no data access)
      StructType schema = spark.table(tableName).schema();

      // Convert to column names
      List<String> columnNames =
          JavaConverters.seqAsJavaList(schema.toSeq()).stream()
              .map(field -> field.name())
              .collect(Collectors.toList());

      if (!columnNames.isEmpty()) {
        log.info("Read schema from Spark catalog for table {}: {}", tableName, columnNames);
        return Optional.of(columnNames);
      }

    } catch (Exception e) {
      log.debug("Not a catalog table or could not read from catalog: {}", e.getMessage());
    }

    return Optional.empty();
  }

  /**
   * Attempts to read schema from file metadata with format auto-detection.
   *
   * @param spark Spark session
   * @param filePath file or directory path
   * @return list of column names if successful, empty otherwise
   */
  private Optional<List<String>> readSchemaFromFileMetadata(SparkSession spark, String filePath) {
    // Try different file formats in order of likelihood and efficiency

    // 1. Parquet (most common, fast metadata-only read)
    Optional<List<String>> schema = tryReadSchema(spark, filePath, "parquet");
    if (schema.isPresent()) {
      return schema;
    }

    // 2. ORC (fast metadata-only read)
    schema = tryReadSchema(spark, filePath, "orc");
    if (schema.isPresent()) {
      return schema;
    }

    // 3. Delta Lake (reads _delta_log metadata)
    schema = tryReadSchema(spark, filePath, "delta");
    if (schema.isPresent()) {
      return schema;
    }

    // 4. Iceberg (reads metadata files)
    schema = tryReadSchema(spark, filePath, "iceberg");
    if (schema.isPresent()) {
      return schema;
    }

    // 5. CSV with header (requires sampling, slower)
    schema = tryReadSchemaWithOptions(spark, filePath, "csv", "header", "true");
    if (schema.isPresent()) {
      return schema;
    }

    // 6. CSV without header (fallback)
    schema = tryReadSchema(spark, filePath, "csv");
    if (schema.isPresent()) {
      return schema;
    }

    // 7. TSV with header (requires sampling, slower)
    schema =
        tryReadSchemaWithMultipleOptions(
            spark,
            filePath,
            "csv",
            new String[] {"delimiter", "header"},
            new String[] {"\t", "true"});
    if (schema.isPresent()) {
      return schema;
    }

    // 8. TSV without header (fallback)
    schema = tryReadSchemaWithOptions(spark, filePath, "csv", "delimiter", "\t");
    if (schema.isPresent()) {
      return schema;
    }

    return Optional.empty();
  }

  /**
   * Tries to read schema using a specific format.
   *
   * @param spark Spark session
   * @param path file path
   * @param format file format (parquet, orc, delta, csv, etc.)
   * @return list of column names if successful, empty otherwise
   */
  private Optional<List<String>> tryReadSchema(SparkSession spark, String path, String format) {
    try {
      StructType schema = spark.read().format(format).load(path).schema();

      List<String> columnNames =
          JavaConverters.seqAsJavaList(schema.toSeq()).stream()
              .map(field -> field.name())
              .collect(Collectors.toList());

      if (!columnNames.isEmpty()) {
        log.info("Read schema from {} file at {}: {}", format, path, columnNames);
        return Optional.of(columnNames);
      }
    } catch (Exception e) {
      log.debug("Failed to read schema as {} format: {}", format, e.getMessage());
    }

    return Optional.empty();
  }

  /**
   * Tries to read schema using a specific format with custom options.
   *
   * @param spark Spark session
   * @param path file path
   * @param format file format
   * @param optionKey option key
   * @param optionValue option value
   * @return list of column names if successful, empty otherwise
   */
  private Optional<List<String>> tryReadSchemaWithOptions(
      SparkSession spark, String path, String format, String optionKey, String optionValue) {
    try {
      StructType schema =
          spark.read().format(format).option(optionKey, optionValue).load(path).schema();

      List<String> columnNames =
          JavaConverters.seqAsJavaList(schema.toSeq()).stream()
              .map(field -> field.name())
              .collect(Collectors.toList());

      if (!columnNames.isEmpty()) {
        log.info(
            "Read schema from {} file with {}={} at {}: {}",
            format,
            optionKey,
            optionValue,
            path,
            columnNames);
        return Optional.of(columnNames);
      }
    } catch (Exception e) {
      log.debug(
          "Failed to read schema as {} format with {}={}: {}",
          format,
          optionKey,
          optionValue,
          e.getMessage());
    }

    return Optional.empty();
  }

  /**
   * Tries to read schema using a specific format with multiple custom options.
   *
   * @param spark Spark session
   * @param path file path
   * @param format file format
   * @param optionKeys array of option keys
   * @param optionValues array of option values
   * @return list of column names if successful, empty otherwise
   */
  private Optional<List<String>> tryReadSchemaWithMultipleOptions(
      SparkSession spark, String path, String format, String[] optionKeys, String[] optionValues) {
    try {
      if (optionKeys.length != optionValues.length) {
        return Optional.empty();
      }

      org.apache.spark.sql.DataFrameReader reader = spark.read().format(format);
      for (int i = 0; i < optionKeys.length; i++) {
        reader = reader.option(optionKeys[i], optionValues[i]);
      }

      StructType schema = reader.load(path).schema();

      List<String> columnNames =
          JavaConverters.seqAsJavaList(schema.toSeq()).stream()
              .map(field -> field.name())
              .collect(Collectors.toList());

      if (!columnNames.isEmpty()) {
        StringBuilder optionsStr = new StringBuilder();
        for (int i = 0; i < optionKeys.length; i++) {
          if (i > 0) optionsStr.append(", ");
          optionsStr.append(optionKeys[i]).append("=").append(optionValues[i]);
        }
        log.info(
            "Read schema from {} file with {} at {}: {}",
            format,
            optionsStr.toString(),
            path,
            columnNames);
        return Optional.of(columnNames);
      }
    } catch (Exception e) {
      log.debug(
          "Failed to read schema as {} format with multiple options: {}", format, e.getMessage());
    }

    return Optional.empty();
  }

  /**
   * Extracts file path from dataset identifier.
   *
   * @param dataset dataset identifier
   * @return file path, or null if extraction fails
   */
  private String extractFilePath(String dataset) {
    if (dataset == null) {
      return null;
    }

    // Handle different dataset identifier formats:
    // 1. "file/path/to/data" -> "/path/to/data"
    // 2. "/path/to/data" -> "/path/to/data"
    // 3. "namespace/path" -> check if second part looks like a path

    if (dataset.startsWith("file/")) {
      return dataset.substring(5); // Remove "file/" prefix
    }

    if (dataset.startsWith("/")) {
      return dataset; // Already a path
    }

    // Check if it contains a slash and second part looks like a path
    int slashIndex = dataset.indexOf('/');
    if (slashIndex > 0 && slashIndex < dataset.length() - 1) {
      String secondPart = dataset.substring(slashIndex + 1);
      if (secondPart.startsWith("/")) {
        return secondPart;
      }
    }

    // Return as-is and let file reading fail gracefully
    return dataset;
  }

  /**
   * Gets the active Spark session if available.
   *
   * @return active Spark session, or empty if none exists
   */
  private Optional<SparkSession> getActiveSparkSession() {
    try {
      SparkSession session = SparkSession.active();
      return Optional.of(session);
    } catch (IllegalStateException e) {
      // No active session
      return Optional.empty();
    }
  }

  /**
   * Clean up old snapshots based on age and count limits.
   *
   * @param snapshots list of snapshots
   * @param currentTime current timestamp
   * @return cleaned up list
   */
  private List<SchemaSnapshot> cleanup(List<SchemaSnapshot> snapshots, long currentTime) {
    // Remove snapshots older than maxAge
    List<SchemaSnapshot> recent =
        snapshots.stream()
            .filter(s -> (currentTime - s.timestamp) <= maxAge)
            .collect(Collectors.toList());

    // Keep only the most recent maxSnapshots
    if (recent.size() > maxSnapshots) {
      recent =
          recent.stream()
              .sorted((a, b) -> Long.compare(b.timestamp, a.timestamp))
              .limit(maxSnapshots)
              .collect(Collectors.toList());
    }

    return recent;
  }

  /**
   * Get all tracked datasets.
   *
   * @return set of dataset identifiers
   */
  public int getTrackedDatasetCount() {
    return datasetSchemas.size();
  }

  /** Clear all tracked schemas. Useful for testing. */
  public void clear() {
    datasetSchemas.clear();
  }
}
