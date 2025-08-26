package com.linkedin.datahub.graphql.types.form;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import java.util.List;

/*
 * This class is meant to take in a Filter object and return a JSON stringified logical predicate.
 * Currently, this is used for form asset assignment where we want to display a logical predicate
 * to the user, but we store a Filter object to use with elastic search.
 */
public class FilterConverter {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static String convertFilterToJsonPredicate(Filter filter) throws Exception {
    ObjectNode rootNode = MAPPER.createObjectNode();
    buildPredicateNode(rootNode, filter);
    return MAPPER.writeValueAsString(rootNode);
  }

  private static void buildPredicateNode(ObjectNode node, Filter filter) {
    if (filter.getOr() != null) {
      if (filter.getOr().size() == 1) {
        // If there's only one ConjunctiveCriterion, flatten the structure
        ConjunctiveCriterion cc = filter.getOr().get(0);
        buildConjunctiveCriterionNode(node, cc);
      } else {
        node.put("operator", "or");
        ArrayNode operandsNode = node.putArray("operands");
        for (ConjunctiveCriterion cc : filter.getOr()) {
          ObjectNode ccNode = operandsNode.addObject();
          buildConjunctiveCriterionNode(ccNode, cc);
        }
      }
    } else if (filter.getCriteria() != null && !filter.getCriteria().isEmpty()) {
      buildCriteriaNode(node, filter.getCriteria());
    }
  }

  private static void buildConjunctiveCriterionNode(ObjectNode node, ConjunctiveCriterion cc) {
    buildCriteriaNode(node, cc.getAnd());
  }

  private static void buildCriteriaNode(ObjectNode node, List<Criterion> criteria) {
    node.put("operator", "and");
    ArrayNode operandsNode = node.putArray("operands");
    for (Criterion criterion : criteria) {
      operandsNode.add(buildCriterionNode(criterion));
    }
  }

  private static ObjectNode buildCriterionNode(Criterion criterion) {
    ObjectNode criterionNode = MAPPER.createObjectNode();
    criterionNode.put("property", criterion.getField().replace(".keyword", ""));
    criterionNode.put(
        "operator", convertCondition(criterion.getCondition(), criterion.isNegated()));
    ArrayNode valuesNode = criterionNode.putArray("values");
    if (criterion.getValues() != null) {
      for (String value : criterion.getValues()) {
        valuesNode.add(value);
      }
    } else if (criterion.getValue() != null) {
      valuesNode.add(criterion.getValue());
    }
    return criterionNode;
  }

  private static String convertCondition(Condition condition, boolean negated) {
    switch (condition) {
      case EQUAL:
      case IN:
        return negated ? "not_equals" : "equals";
      case EXISTS:
        return "exists";
      default:
        return "unknown";
    }
  }
}
