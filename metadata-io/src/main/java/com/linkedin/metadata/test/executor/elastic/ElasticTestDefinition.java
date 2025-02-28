package com.linkedin.metadata.test.executor.elastic;

import com.linkedin.metadata.test.definition.TestDefinition;
import com.linkedin.metadata.test.definition.operator.Predicate;
import java.util.List;

public class ElasticTestDefinition {

  private final TestDefinition testDefinition;
  private final Predicate selectionFilters;
  private final Predicate passingFilters;
  private final Predicate failingFilters;

  public ElasticTestDefinition(
      TestDefinition testDefinition,
      Predicate selectionFilters,
      Predicate passingFilters,
      Predicate failingFilters) {
    this.testDefinition = testDefinition;
    this.selectionFilters = selectionFilters;
    this.passingFilters = passingFilters;
    this.failingFilters = failingFilters;
  }

  public List<String> getSelectedEntityTypes() {
    return testDefinition.getOn().getEntityTypes();
  }

  public Predicate getSelectionFilters() {
    return selectionFilters;
  }

  public Predicate getPassingFilters() {
    return passingFilters;
  }

  public Predicate getPassingFilters(String entityType) {
    return passingFilters;
  }

  public Predicate getFailingFilters() {
    return failingFilters;
  }

  public Predicate getFailingFilters(String entityType) {
    return failingFilters;
  }
}
