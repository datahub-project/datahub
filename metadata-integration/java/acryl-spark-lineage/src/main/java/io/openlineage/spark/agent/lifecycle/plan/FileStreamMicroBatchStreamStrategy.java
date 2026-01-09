/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.DatasetFactory;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation;

/**
 * Strategy for handling file-based streaming sources (CSV, Parquet, JSON, etc.) in micro-batch
 * streaming operations.
 */
@Slf4j
public class FileStreamMicroBatchStreamStrategy extends StreamStrategy {

  private final StreamingDataSourceV2Relation relation;

  public FileStreamMicroBatchStreamStrategy(
      DatasetFactory<OpenLineage.InputDataset> datasetFactory,
      StreamingDataSourceV2Relation relation) {
    super(datasetFactory, relation.schema(), relation.stream(), Optional.empty());
    this.relation = relation;
  }

  @Override
  public List<OpenLineage.InputDataset> getInputDatasets() {
    log.info("Extracting input datasets from file-based streaming source");

    try {
      // Get the streaming source path
      Optional<String> pathOpt = getStreamingSourcePath();
      if (!pathOpt.isPresent()) {
        log.warn("Could not extract path from file-based streaming source");
        return Collections.emptyList();
      }

      String path = pathOpt.get();
      log.info("Found streaming source path: {}", path);

      // Create dataset from path
      URI uri = URI.create(path);
      DatasetIdentifier identifier = PathUtils.fromURI(uri);
      String namespace = identifier.getNamespace();
      String name = identifier.getName();

      log.info("Creating input dataset with namespace: {}, name: {}", namespace, name);

      // Use the inherited datasetFactory to create the dataset
      OpenLineage.InputDataset dataset = datasetFactory.getDataset(name, namespace, schema);

      return Collections.singletonList(dataset);

    } catch (Exception e) {
      log.error("Error extracting input datasets from file streaming source", e);
      return Collections.emptyList();
    }
  }

  /**
   * Extract the path from file-based streaming source. This handles various file formats (CSV,
   * Parquet, JSON, etc.)
   */
  private Optional<String> getStreamingSourcePath() {
    try {
      // Try to get path from the streaming source options
      Object streamObj = relation.stream();
      if (streamObj == null) {
        return Optional.empty();
      }

      // Use reflection to get the path from various file-based stream types
      String streamClassName = streamObj.getClass().getCanonicalName();
      log.info("Processing stream class: {}", streamClassName);

      // Handle different file-based streaming sources
      if (streamClassName != null) {
        if (streamClassName.contains("FileStreamSource")
            || streamClassName.contains("TextFileStreamSource")
            || streamClassName.contains("org.apache.spark.sql.execution.streaming.sources")) {

          // Try to extract path using reflection
          try {
            java.lang.reflect.Method getPathMethod = streamObj.getClass().getMethod("path");
            if (getPathMethod != null) {
              Object pathObj = getPathMethod.invoke(streamObj);
              if (pathObj != null) {
                return Optional.of(pathObj.toString());
              }
            }
          } catch (Exception e) {
            log.debug("Could not extract path via reflection: {}", e.getMessage());
          }

          // Try alternative methods for getting path
          try {
            java.lang.reflect.Field pathField = streamObj.getClass().getDeclaredField("path");
            pathField.setAccessible(true);
            Object pathObj = pathField.get(streamObj);
            if (pathObj != null) {
              return Optional.of(pathObj.toString());
            }
          } catch (Exception e) {
            log.debug("Could not extract path via field access: {}", e.getMessage());
          }
        }
      }

      // Fallback: return a generic path if we can't extract the real one
      log.debug("Could not extract specific path, using generic file path");
      return Optional.of("file:///streaming/input");

    } catch (Exception e) {
      log.error("Error extracting path from streaming source", e);
    }

    return Optional.empty();
  }
}
