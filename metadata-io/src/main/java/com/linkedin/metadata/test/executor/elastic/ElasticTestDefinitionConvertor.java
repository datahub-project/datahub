package com.linkedin.metadata.test.executor.elastic;

import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.test.definition.TestDefinition;
import com.linkedin.metadata.test.definition.operator.Operand;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.operator.Predicate;
import java.util.List;

public class ElasticTestDefinitionConvertor {

  private final EntityRegistry entityRegistry;

  public ElasticTestDefinitionConvertor(EntityRegistry entityRegistry) {
    this.entityRegistry = entityRegistry;
  }

  public boolean canSelect(TestDefinition testDefinition) {
    for (String entityType : testDefinition.getOn().getEntityTypes()) {
      List<SearchableFieldSpec> fieldSpecs =
          entityRegistry.getEntitySpec(entityType).getSearchableFieldSpecs();
      // if there are no operands, then we can convert
      if (testDefinition.getOn().getConditions() != null
          && testDefinition.getOn().getConditions().getOperands().size() > 0) {
        return PredicateToFilter.canConvertPredicateToFilter(
            testDefinition.getOn().getConditions());
      }
      return true;
      // Walk over all the operands and check if the field is searchable
      //      if (!
      // testDefinition.getOn().getConditions().getOperands().get().stream().allMatch(operand -> {
      //        String fieldName = operand.getName();
      //        if (fieldSpecs.stream().anyMatch(fieldSpec ->
      // fieldSpec.getPath().toString().equals(fieldName))) {
      //          return true;
      //        }
      //        return false;
      //      })) {
      //        return false;
      //      };
    }
    // All fields are searchable
    return true;
  }

  public boolean canEvaluate(TestDefinition testDefinition) {
    boolean canSelect = canSelect(testDefinition);
    boolean canEvaluate = false;
    if ((testDefinition.getRules() == null)
        || (testDefinition.getRules().getOperands().size() == 0)) {
      canEvaluate = true;
    } else {
      canEvaluate = PredicateToFilter.canConvertPredicateToFilter(testDefinition.getRules());
    }

    return canEvaluate && canSelect;
  }

  public ElasticTestDefinition convert(TestDefinition testDefinition) {
    Filter selectionFilters =
        PredicateToFilter.transformPredicateToFilter(testDefinition.getOn().getConditions());
    if (selectionFilters == null) {
      return null;
    }
    if (testDefinition.getRules() == null) {
      return new ElasticTestDefinition(testDefinition, selectionFilters, selectionFilters, null);
    } else {
      Predicate passingPredicate =
          new Predicate(
              OperatorType.AND,
              List.of(
                  new Operand(0, testDefinition.getOn().getConditions()),
                  new Operand(1, testDefinition.getRules())));
      Filter passingFilters = PredicateToFilter.transformPredicateToFilter(passingPredicate);
      return new ElasticTestDefinition(testDefinition, selectionFilters, passingFilters, null);
    }
  }
}
