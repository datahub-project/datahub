package com.linkedin.metadata.test.executor.elastic;

import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.test.definition.TestDefinition;
import com.linkedin.metadata.test.definition.operator.Operand;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.test.query.TestQuery;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ElasticTestDefinitionConvertor {

  private final EntityRegistry entityRegistry;

  public ElasticTestDefinitionConvertor(EntityRegistry entityRegistry) {
    this.entityRegistry = entityRegistry;
  }

  public boolean canSelect(TestDefinition testDefinition) {
    for (String entityType : testDefinition.getOn().getEntityTypes()) {
      Set<TestQuery> queries =
          Predicate.extractQueriesForPredicate(testDefinition.getOn().getConditions());
      Map<PathSpec, String> fieldPaths =
          entityRegistry.getEntitySpec(entityType).getSearchableFieldPathMap();
      if (!queries.stream()
          .map(TestQuery::getQueryParts)
          .map(PathSpec::new)
          .allMatch(fieldPaths::containsKey)) {
        return false;
      }
    }
    return true;
  }

  public boolean canEvaluate(TestDefinition testDefinition) {
    if ((testDefinition.getRules() != null)
        && (testDefinition.getRules().getOperands().size() != 0)) {
      for (String entityType : testDefinition.getOn().getEntityTypes()) {
        Set<TestQuery> queries = Predicate.extractQueriesForPredicate(testDefinition.getRules());
        Map<PathSpec, String> fieldPaths =
            entityRegistry.getEntitySpec(entityType).getSearchableFieldPathMap();
        if (!queries.stream()
            .map(TestQuery::getQueryParts)
            .map(PathSpec::new)
            .allMatch(fieldPaths::containsKey)) {
          return false;
        }
      }
    }
    return true;
  }

  public ElasticTestDefinition convert(TestDefinition testDefinition) {
    Predicate selectionFilters = testDefinition.getOn().getConditions();
    if (testDefinition.getRules() == null) {
      return new ElasticTestDefinition(testDefinition, selectionFilters, selectionFilters, null);
    } else {
      Predicate passingPredicate =
          new Predicate(
              OperatorType.AND,
              List.of(
                  new Operand(0, testDefinition.getOn().getConditions()),
                  new Operand(1, testDefinition.getRules())));
      return new ElasticTestDefinition(testDefinition, selectionFilters, passingPredicate, null);
    }
  }
}
