package com.linkedin.metadata.test.executor.elastic;

import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.test.definition.TestDefinition;
import com.linkedin.metadata.test.definition.operator.Operand;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.test.query.TestQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticTestDefinitionConvertor {

  private final EntityRegistry entityRegistry;

  public ElasticTestDefinitionConvertor(EntityRegistry entityRegistry) {
    this.entityRegistry = entityRegistry;
  }

  public boolean canSelect(TestDefinition testDefinition) {
    for (String entityType : testDefinition.getOn().getEntityTypes()) {
      if (testDefinition.getOn().getConditions() == null) {
        continue;
      }
      Set<TestQuery> queries =
          Predicate.extractQueriesForPredicate(testDefinition.getOn().getConditions());
      Map<PathSpec, String> fieldPaths =
          entityRegistry.getEntitySpec(entityType).getSearchableFieldPathMap();
      if (isSearchable(queries, fieldPaths)) {
        log.warn(
            "Unable to select for queries: {}, available fieldPaths: {} for entity {}",
            queries,
            fieldPaths,
            entityType);
        return false;
      }
    }
    return true;
  }

  private static boolean isSearchable(Set<TestQuery> queries, Map<PathSpec, String> fieldPaths) {
    return !queries.stream()
        .filter(q -> !q.getQuery().equals("_entityType"))
        .map(TestQuery::getQueryParts)
        .map(PathSpec::new)
        .allMatch(fieldPaths::containsKey);
  }

  public List<String> explainSelect(TestDefinition testDefinition) {
    List<String> explanations = new ArrayList<>();
    for (String entityType : testDefinition.getOn().getEntityTypes()) {
      if (testDefinition.getOn().getConditions() == null) {
        continue;
      }
      Set<TestQuery> queries =
          Predicate.extractQueriesForPredicate(testDefinition.getOn().getConditions());
      Map<PathSpec, String> fieldPaths =
          entityRegistry.getEntitySpec(entityType).getSearchableFieldPathMap();
      queries.stream()
          .map(TestQuery::getQueryParts)
          .map(PathSpec::new)
          .filter(pathSpec -> !fieldPaths.containsKey(pathSpec))
          .forEach(
              pathSpec ->
                  explanations.add(
                      String.format(
                          "Query path %s is not a searchable field, unable to "
                              + "select using ElasticSearchExecutor.",
                          pathSpec)));
    }
    if (explanations.isEmpty()) {
      explanations.add("Able to select using ElasticSearchExecutor, all fields are searchable.");
    } else {
      explanations.add("Default selector will be used.");
    }
    return explanations;
  }

  public boolean canEvaluate(TestDefinition testDefinition) {
    if ((testDefinition.getRules() != null)
        && (testDefinition.getRules().getOperands().size() != 0)) {
      for (String entityType : testDefinition.getOn().getEntityTypes()) {
        Set<TestQuery> queries = Predicate.extractQueriesForPredicate(testDefinition.getRules());
        Map<PathSpec, String> fieldPaths =
            entityRegistry.getEntitySpec(entityType).getSearchableFieldPathMap();
        if (isSearchable(queries, fieldPaths)) {
          log.warn(
              "Unable to evaluate for queries: {}, available fieldPaths: {} for entity {}",
              queries,
              fieldPaths,
              entityType);
          return false;
        }
      }
    }
    return true;
  }

  public List<String> explainEvaluate(TestDefinition testDefinition) {
    List<String> explanations = new ArrayList<>();
    if ((testDefinition.getRules() != null)
        && (testDefinition.getRules().getOperands().size() != 0)) {
      for (String entityType : testDefinition.getOn().getEntityTypes()) {
        Set<TestQuery> queries = Predicate.extractQueriesForPredicate(testDefinition.getRules());
        Map<PathSpec, String> fieldPaths =
            entityRegistry.getEntitySpec(entityType).getSearchableFieldPathMap();
        queries.stream()
            .map(TestQuery::getQueryParts)
            .map(PathSpec::new)
            .filter(pathSpec -> !fieldPaths.containsKey(pathSpec))
            .forEach(
                pathSpec ->
                    explanations.add(
                        String.format(
                            "Query path %s is not a searchable field, unable to "
                                + "evaluate using ElasticSearchExecutor.",
                            pathSpec)));
      }
    }
    if (explanations.isEmpty()) {
      explanations.add("Able to evaluate using ElasticSearchExecutor, all fields are searchable.");
    } else {
      explanations.add("Default evaluator will be used.");
    }
    return explanations;
  }

  public ElasticTestDefinition convert(TestDefinition testDefinition) {
    Predicate selectionFilters = testDefinition.getOn().getConditions();
    if (testDefinition.getRules() == null) {
      return new ElasticTestDefinition(testDefinition, selectionFilters, selectionFilters, null);
    } else if (selectionFilters == null) {
      return new ElasticTestDefinition(testDefinition, null, testDefinition.getRules(), null);
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
