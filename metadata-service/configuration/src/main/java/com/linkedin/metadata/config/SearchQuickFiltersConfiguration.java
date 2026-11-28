package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "searchQuickFilters" configuration block in application.yaml */
@Data
public class SearchQuickFiltersConfiguration {
  /** Max number of platform quick filters, ranked by asset count */
  public Integer platformCount;

  /** Max number of source entity type quick filters, taken in prioritizedSourceEntityTypes order */
  public Integer sourceEntityCount;

  /**
   * Max number of DataHub entity type quick filters, taken in prioritizedDatahubEntityTypes order
   */
  public Integer datahubEntityCount;
}
