package com.linkedin.metadata.test.executor.elastic;

import static com.linkedin.metadata.test.definition.operator.OperatorType.*;

import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.test.definition.expression.Query;
import com.linkedin.metadata.test.definition.literal.DateLiteral;
import com.linkedin.metadata.test.definition.literal.StringListLiteral;
import com.linkedin.metadata.test.definition.operator.Operand;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.operator.Predicate;
import java.util.Collections;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PredicateToFilter {

  private static final Map<String, String> QUERY_SEARCH_FIELD_MAP =
      Map.of(
          "dataPlatformInstance.platform", "platform",
          "subTypes.typeNames", "typeNames",
          "urn", "urn.keyword",
          "globalTags.tags.tag", "tags",
          "glossaryTerms.terms.urn", "glossaryTerms",
          //      "glossaryTerms.terms.urn.glossaryTermInfo.parentNode", null,
          "domains.domains", "domains",
          "ownership.owners.owner", "owners",
          "container.container", "container",
          "operation.lastUpdatedTimestamp", "lastOperationTime");

  private static String getSearchFieldName(String query) {
    if (QUERY_SEARCH_FIELD_MAP.containsKey(query)) {
      return QUERY_SEARCH_FIELD_MAP.get(query);
    }
    return null;
  }

  public static boolean canConvertPredicateToFilter(Predicate predicate) {
    try {
      transformPredicateToFilter(predicate);
    } catch (UnsupportedOperationException e) {
      log.warn("Failed to convert predicate to filter: {}", e.toString());
      return false;
    }
    return true;
  }

  public static Filter transformPredicateToFilter(Predicate predicate) {
    Filter filter = new Filter();
    if (predicate == null) {
      return filter;
    }
    if (predicate.getOperatorType() == OperatorType.OR) {
      ConjunctiveCriterionArray orCriteria = processOrPredicate(predicate);
      filter.setOr(orCriteria);
    } else {
      // For a single AND predicate or any leaf predicate
      ConjunctiveCriterion conjunctiveCriterion = processPredicate(predicate);
      if (conjunctiveCriterion != null) {
        ConjunctiveCriterionArray conjunctiveCriteria = new ConjunctiveCriterionArray();
        conjunctiveCriteria.add(conjunctiveCriterion);
        filter.setOr(conjunctiveCriteria);
      }
    }

    return filter;
  }

  private static ConjunctiveCriterionArray processOrPredicate(Predicate predicate) {
    ConjunctiveCriterionArray orCriteria = new ConjunctiveCriterionArray();
    // Assuming OR predicate contains a list of AND predicates or leaf predicates
    for (Operand operand : predicate.getOperands().get()) {
      if (operand.getExpression() instanceof Predicate) {
        Predicate subPredicate = (Predicate) operand.getExpression();
        ConjunctiveCriterion conjunctiveCriterion = processPredicate(subPredicate);
        if (conjunctiveCriterion != null) {
          orCriteria.add(conjunctiveCriterion);
        }
      }
    }
    return orCriteria;
  }

  private static ConjunctiveCriterion processPredicate(Predicate predicate) {
    if (predicate.getOperatorType() == OperatorType.AND) {
      ConjunctiveCriterion conjunctiveCriterion =
          new ConjunctiveCriterion().setAnd(new CriterionArray());
      // List<Criterion> criteriaList = new ArrayList<>();
      for (Operand operand : predicate.getOperands().get()) {
        if (operand.getExpression() instanceof Predicate) {
          Predicate subPredicate = (Predicate) operand.getExpression();
          if (subPredicate.getOperatorType() != OperatorType.AND
              && subPredicate.getOperatorType() != OperatorType.OR) {
            // Leaf predicate
            Criterion criterion = convertLeafPredicateToCriterion(subPredicate);
            if (criterion != null) {
              CriterionArray criteriaList = conjunctiveCriterion.getAnd();
              if (criteriaList.isEmpty()) {
                criteriaList = new CriterionArray();
              }
              criteriaList.add(criterion);
              conjunctiveCriterion.setAnd(criteriaList);
            }
          } else {
            // Nested AND or OR, call recursively
            ConjunctiveCriterion nestedConjunctiveCriterion = processPredicate(subPredicate);
            if (nestedConjunctiveCriterion != null) {
              // Wrap nested criteria in a new conjunctive criterion if needed
              CriterionArray criterionArray = new CriterionArray();
              criterionArray.addAll(nestedConjunctiveCriterion.getAnd());
              // ConjunctiveCriterion wrapperConjunctiveCriterion = new
              // ConjunctiveCriterion().setAnd(criterionArray);
              conjunctiveCriterion.getAnd().addAll(criterionArray);
            }
          }
        }
      }
      return conjunctiveCriterion;
    } else {
      // Leaf predicate
      Criterion leafCriterion = convertLeafPredicateToCriterion(predicate);
      if (leafCriterion != null) {
        return new ConjunctiveCriterion()
            .setAnd(new CriterionArray(Collections.singletonList(leafCriterion)));
      }
    }
    return null;
  }

  private static Criterion convertLeafPredicateToCriterion(Predicate predicate) {
    // Implement conversion based on your predicate structure
    // For example, for an EQUAL operator type:
    switch (predicate.getOperatorType()) {
      case EXISTS:
        {
          Operand firstOperand = predicate.getOperands().get(0);
          if (firstOperand.getExpression() instanceof Query) {
            Query query = (Query) firstOperand.getExpression();
            String fieldName = getSearchFieldName(query.getQuery().getQuery());
            if (fieldName == null) {
              throw new UnsupportedOperationException(
                  "Unsupported field in query: " + query.getQuery().getQuery());
            }
            return new Criterion().setField(fieldName).setCondition(Condition.EXISTS);
          }
        }
      case ANY_EQUALS:
        {
          Operand firstOperand = predicate.getOperands().get(0);
          Operand valueOperand = predicate.getOperands().get(1);
          if (firstOperand.getExpression() instanceof Query) {
            Query query = (Query) firstOperand.getExpression();
            String fieldName = getSearchFieldName(query.getQuery().getQuery());
            if (fieldName == null) {
              throw new UnsupportedOperationException(
                  "Unsupported field in query: " + query.getQuery().getQuery());
            }
            StringArray values = getSearchValueField(valueOperand);
            return new Criterion()
                .setField(fieldName)
                .setValue(values.get(0), SetMode.IGNORE_NULL)
                .setValues(values)
                .setCondition(Condition.EQUAL);
          }
        }
        break;
      case CONTAINS_ANY:
        {
          Operand firstOperand = predicate.getOperands().get(0);
          if (firstOperand.getExpression() instanceof Query) {
            Query query = (Query) firstOperand.getExpression();
            String fieldName = getSearchFieldName(query.getQuery().getQuery());
            if (fieldName == null) {
              throw new UnsupportedOperationException(
                  "Unsupported field in query: " + query.getQuery().getQuery());
            }
            Criterion containsCriterion =
                new Criterion().setField(fieldName).setCondition(Condition.IN);
            // single value
            Operand valueOperand = predicate.getOperands().get(1);
            StringArray values = getSearchValueField(valueOperand);
            containsCriterion.setValue(values.get(0)).setValues(values, SetMode.IGNORE_NULL);
            return containsCriterion;
          }
        }
        break;
      case STARTS_WITH:
        {
          Operand firstOperand = predicate.getOperands().get(0);
          Operand valueOperand = predicate.getOperands().get(1);
          if (firstOperand.getExpression() instanceof Query) {
            Query query = (Query) firstOperand.getExpression();
            String fieldName = getSearchFieldName(query.getQuery().getQuery());
            if (fieldName == null) {
              throw new UnsupportedOperationException(
                  "Unsupported field in query: " + query.getQuery().getQuery());
            }
            StringArray values = getSearchValueField(valueOperand);
            return new Criterion()
                .setField(fieldName)
                .setValue(values.get(0), SetMode.IGNORE_NULL)
                .setValues(values)
                .setCondition(Condition.START_WITH);
          }
        }
        break;
      case LESS_THAN:
        {
          Operand firstOperand = predicate.getOperands().get(0);
          Operand valueOperand = predicate.getOperands().get(1);
          if (firstOperand.getExpression() instanceof Query) {
            Query query = (Query) firstOperand.getExpression();
            String fieldName = getSearchFieldName(query.getQuery().getQuery());
            if (fieldName == null) {
              throw new UnsupportedOperationException(
                  "Unsupported field in query: " + query.getQuery().getQuery());
            }
            StringArray values = getSearchValueField(valueOperand);
            return new Criterion()
                .setField(fieldName)
                .setValue(values.get(0), SetMode.IGNORE_NULL)
                .setValues(values)
                .setCondition(Condition.LESS_THAN);
          }
        }
        break;
      case GREATER_THAN:
        {
          Operand firstOperand = predicate.getOperands().get(0);
          Operand valueOperand = predicate.getOperands().get(1);
          if (firstOperand.getExpression() instanceof Query) {
            Query query = (Query) firstOperand.getExpression();
            String fieldName = getSearchFieldName(query.getQuery().getQuery());
            if (fieldName == null) {
              throw new UnsupportedOperationException(
                  "Unsupported field in query: " + query.getQuery().getQuery());
            }
            StringArray values = getSearchValueField(valueOperand);
            return new Criterion()
                .setField(fieldName)
                .setValue(values.get(0), SetMode.IGNORE_NULL)
                .setValues(values)
                .setCondition(Condition.GREATER_THAN);
          }
        }
        break;
      case CONTAINS_STR:
        {
          Operand firstOperand = predicate.getOperands().get(0);
          Operand valueOperand = predicate.getOperands().get(1);
          if (firstOperand.getExpression() instanceof Query) {
            Query query = (Query) firstOperand.getExpression();
            String fieldName = getSearchFieldName(query.getQuery().getQuery());
            if (fieldName == null) {
              throw new UnsupportedOperationException(
                  "Unsupported field in query: " + query.getQuery().getQuery());
            }
            StringArray values = getSearchValueField(valueOperand);
            return new Criterion()
                .setField(fieldName)
                .setValue(values.get(0), SetMode.IGNORE_NULL)
                .setValues(values)
                .setCondition(Condition.CONTAIN);
          }
        }
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported operator type: " + predicate.getOperatorType());
    }
    // Extend to handle other leaf operator types
    throw new UnsupportedOperationException(
        "Unsupported operator type: " + predicate.getOperatorType());
  }

  private static StringArray getSearchValueField(Operand valueOperand) {
    StringArray valuesArray = new StringArray();
    if (valueOperand.getExpression() instanceof StringListLiteral) {
      StringListLiteral stringListLiteral = (StringListLiteral) valueOperand.getExpression();
      valuesArray.addAll(stringListLiteral.getValues());
      return valuesArray;
    } else if (valueOperand.getExpression() instanceof DateLiteral) {
      DateLiteral dateLiteral = (DateLiteral) valueOperand.getExpression();
      valuesArray.add(dateLiteral.resolveValue());
      return valuesArray;
    } else {
      throw new UnsupportedOperationException(
          "Unsupported value type: " + valueOperand.getExpression().getClass().getName());
    }
  }
}
