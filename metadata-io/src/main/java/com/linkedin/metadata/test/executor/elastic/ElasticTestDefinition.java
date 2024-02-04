package com.linkedin.metadata.test.executor.elastic;

import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.test.definition.TestDefinition;
import java.util.List;

public class ElasticTestDefinition {

  private final TestDefinition testDefinition;
  private final Filter selectionFilters;
  private final Filter passingFilters;
  private final Filter failingFilters;

  public ElasticTestDefinition(
      TestDefinition testDefinition,
      Filter selectionFilters,
      Filter passingFilters,
      Filter failingFilters) {
    this.testDefinition = testDefinition;
    this.selectionFilters = selectionFilters;
    this.passingFilters = passingFilters;
    this.failingFilters = failingFilters;
  }

  public List<String> getSelectedEntityTypes() {
    return testDefinition.getOn().getEntityTypes();
  }

  public Filter getSelectionFilters(String entityType) {
    return selectionFilters;
  }

  public Filter getPassingFilters(String entityType) {
    return passingFilters;
  }

  public Filter getFailingFilters(String entityType) {
    return failingFilters;
  }
}
