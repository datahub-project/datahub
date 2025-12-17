/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class ColumnLineageConfig {
  /**
   * Determines if the dataset dependencies (FILTER, SORT BY, GROUP BY, WINDOW, etc.) should be
   * represented as field dependencies. WARNING: This flag is temporary. It is going to default to
   * true in future versions and eventually removed.
   */
  // TODO #3084: For the release 1.26.0 this flag should default to true
  // TODO #3084: Three releases later (1.29.0), this flag should be removed and the behavior should
  // reflect it set to true
  private boolean datasetLineageEnabled = false;

  /** Boolean version of datasetLineageEnabled for backwards compatibility with OpenLineage 1.38. */
  public Boolean getDatasetLineageEnabled() {
    return datasetLineageEnabled;
  }

  /**
   * Enables RDD lineage correction using positional schema mapping. When DataFrames are converted
   * to RDDs and back (df.rdd → createDataFrame), the logical plan is lost, resulting in
   * self-referential column lineage. This flag enables detection and correction of such cases using
   * positional mapping based on schema history. Default: false (disabled for backwards
   * compatibility).
   */
  private boolean rddLineageCorrectionEnabled = false;

  /**
   * Maximum age (in milliseconds) for schema snapshots used in RDD lineage correction. Older
   * snapshots are discarded. Default: 3600000 (1 hour).
   */
  private long rddLineageSchemaMaxAge = 3600000L;

  /**
   * Maximum number of schema snapshots to retain per dataset for RDD lineage correction. Default:
   * 10.
   */
  private int rddLineageSchemaMaxSnapshots = 10;

  /**
   * Maximum size of schema (number of fields) to include column lineage. Large schemas are excluded
   * to avoid performance issues. Default: null (no limit).
   */
  private Integer schemaSizeLimit = null;
}
