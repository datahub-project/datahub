package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "columnViews" configuration block in application.yaml. */
@Data
public class ColumnViewsConfiguration {
  /**
   * Maximum related items returned per schema field by the Column View relationship preview query;
   * also the ceiling for a column's {@code display.maxItems} at query time.
   */
  public int relationshipPreviewLimit;
}
