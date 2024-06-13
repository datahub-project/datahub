package com.linkedin.datahub.graphql.instrumentation;

import graphql.analysis.FieldComplexityCalculator;
import graphql.analysis.FieldComplexityEnvironment;
import graphql.language.Field;
import graphql.language.FragmentSpread;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataHubFieldComplexityCalculator implements FieldComplexityCalculator {

  private static final String COUNT_ARG = "count";
  private static final String INPUT_ARG = "input";
  private static final String SEARCH_RESULTS_FIELD = "searchResults";
  private static final String ENTITY_FIELD = "entity";
  private static final String SEARCH_RESULT_FIELDS_FIELD = "searchResultFields";
  private static final String GRAPHQL_QUERY_TYPE = "Query";

  @SuppressWarnings("rawtypes")
  @Override
  public int calculate(FieldComplexityEnvironment environment, int childComplexity) {
    int complexity = 1;
    Map<String, Object> args = environment.getArguments();
    if (args.containsKey(INPUT_ARG)) {
      Map<String, Object> input = (Map<String, Object>) args.get(INPUT_ARG);
      if (input.containsKey(COUNT_ARG) && (Integer) input.get(COUNT_ARG) > 1) {
        Integer count = (Integer) input.get(COUNT_ARG);
        Field field = environment.getField();
        complexity += countRecursiveLineageComplexity(count, field);
      }
    }
    if (GRAPHQL_QUERY_TYPE.equals(environment.getParentType().getName())) {
      log.info(
          "Query complexity for query: {} is {}",
          environment.getField().getName(),
          complexity + childComplexity);
    }
    return complexity + childComplexity;
  }

  private int countRecursiveLineageComplexity(Integer count, Field field) {
    List<Selection> subFields = field.getSelectionSet().getSelections();
    Optional<FragmentSpread> searchResultsFieldsField =
        subFields.stream()
            .filter(selection -> selection instanceof Field)
            .map(selection -> (Field) selection)
            .filter(subField -> SEARCH_RESULTS_FIELD.equals(subField.getName()))
            .map(Field::getSelectionSet)
            .map(SelectionSet::getSelections)
            .flatMap(List::stream)
            .filter(selection -> selection instanceof Field)
            .map(selection -> (Field) selection)
            .filter(subField -> ENTITY_FIELD.equals(subField.getName()))
            .map(Field::getSelectionSet)
            .map(SelectionSet::getSelections)
            .flatMap(List::stream)
            .filter(selection -> selection instanceof FragmentSpread)
            .map(selection -> (FragmentSpread) selection)
            .filter(subField -> SEARCH_RESULT_FIELDS_FIELD.equals(subField.getName()))
            .findFirst();
    if (searchResultsFieldsField.isPresent()) {
      // This fragment includes 2 lineage queries, we account for this additional complexity by
      // multiplying
      // by the count of entities attempting to be returned
      return 2 * count;
    }
    return 0;
  }
}
