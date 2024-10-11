package com.linkedin.metadata.utils.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.test.definition.expression.Expression;
import com.linkedin.metadata.test.definition.expression.Query;
import com.linkedin.metadata.test.definition.literal.StringListLiteral;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.operator.Predicate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AcrylSearchUtils {

  public static final String KEYWORD_SUFFIX = ".keyword";

  private AcrylSearchUtils() {}

  /* TODO: Copied from SearchUtils, extract to shared utility */
  @Nonnull
  public static Filter convertToV2Filter(@Nonnull Filter filter) {
    if (filter.hasOr()) {
      return filter;
    } else if (filter.hasCriteria()) {
      // Convert criteria to an OR
      return new Filter()
          .setOr(
              new ConjunctiveCriterionArray(
                  ImmutableList.of(new ConjunctiveCriterion().setAnd(filter.getCriteria()))));
    }
    throw new IllegalArgumentException(
        String.format(
            "Illegal filter provided! Neither 'or' nor 'criteria' fields were populated for filter %s",
            filter));
  }

  @Nullable
  public static Predicate convertFilterToPredicate(@Nonnull Filter filter) {
    Filter filterV2 = convertToV2Filter(filter);
    if (filterV2.getOr() == null || filterV2.getOr().isEmpty()) {
      return null;
    }
    List<Expression> orList = new ArrayList<>();
    for (ConjunctiveCriterion conjunctiveCriterion : filterV2.getOr()) {
      List<Expression> andList = new ArrayList<>();
      for (Criterion criterion : conjunctiveCriterion.getAnd()) {
        Condition condition = criterion.getCondition();
        List<OperatorType> operatorTypes = new ArrayList<>();
        boolean negated = false;
        switch (condition) {
          case EQUAL:
          case CONTAIN:
          case IN:
            operatorTypes.add(OperatorType.ANY_EQUALS);
            break;
          case EXISTS:
            operatorTypes.add(OperatorType.EXISTS);
            break;
          case LESS_THAN:
            operatorTypes.add(OperatorType.LESS_THAN);
            break;
          case GREATER_THAN:
            operatorTypes.add(OperatorType.GREATER_THAN);
            break;
          case GREATER_THAN_OR_EQUAL_TO:
            operatorTypes.add(OperatorType.GREATER_THAN);
            operatorTypes.add(OperatorType.ANY_EQUALS);
            break;
          case LESS_THAN_OR_EQUAL_TO:
            operatorTypes.add(OperatorType.LESS_THAN);
            operatorTypes.add(OperatorType.ANY_EQUALS);
            break;
          case START_WITH:
            operatorTypes.add(OperatorType.STARTS_WITH);
            break;
          case END_WITH:
            operatorTypes.add(OperatorType.REGEX_MATCH);
            break;
          case IS_NULL:
            operatorTypes.add(OperatorType.EXISTS);
            negated = true;
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported criterion condition: " + condition);
        }
        for (OperatorType op : operatorTypes) {
          Predicate leaf;
          // Strip keyword suffix, we will add it back in later if we need it
          // TODO: Evaluate if ResolverUtils should still be doing this, seems like the wrong place
          // for that logic
          String criterionField = criterion.getField().replace(KEYWORD_SUFFIX, "");
          // Only one scenario where Regex is supported, so we handle it specially to specify values
          // with regex
          if (OperatorType.REGEX_MATCH.equals(op)) {
            Query query = new Query(criterionField);
            List<String> values =
                !criterion.getValues().isEmpty()
                    ? criterion.getValues()
                    : Collections.singletonList(criterion.getValue());
            StringListLiteral literal =
                new StringListLiteral(
                    values.stream()
                        .map(AcrylSearchUtils::endsWithRegex)
                        .collect(Collectors.toList()));
            List<Expression> ops = ImmutableList.of(query, literal);
            leaf = Predicate.of(op, ops, negated);
          } else {
            Query query = new Query(criterionField);
            List<String> values =
                !criterion.getValues().isEmpty()
                    ? criterion.getValues()
                    : Collections.singletonList(criterion.getValue());
            StringListLiteral literal = new StringListLiteral(values);
            List<Expression> ops = ImmutableList.of(query, literal);
            leaf = Predicate.of(op, ops, negated);
          }
          andList.add(leaf);
        }
        Predicate and = Predicate.of(OperatorType.AND, andList);
        orList.add(and);
      }
    }
    return Predicate.of(OperatorType.OR, orList);
  }

  private static String endsWithRegex(String value) {
    return String.format(".*%s", value);
  }
}
