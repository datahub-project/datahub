package com.linkedin.metadata.utils;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class CriterionUtils {
  private CriterionUtils() {}

  public static Criterion buildExistsCriterion(@Nonnull String field) {
    return buildCriterion(field, Condition.EXISTS, false, Collections.emptyList());
  }

  public static Criterion buildNotExistsCriterion(@Nonnull String field) {
    return buildCriterion(field, Condition.EXISTS, true, Collections.emptyList());
  }

  public static Criterion buildIsNullCriterion(@Nonnull String field) {
    return buildCriterion(field, Condition.IS_NULL, false, Collections.emptyList());
  }

  public static Criterion buildIsNotNullCriterion(@Nonnull String field) {
    return buildCriterion(field, Condition.IS_NULL, true, Collections.emptyList());
  }

  public static Criterion buildCriterion(
      @Nonnull String field, @Nonnull Condition condition, String... values) {
    return buildCriterion(
        field,
        condition,
        null,
        values == null
            ? Collections.emptyList()
            : Arrays.stream(values).collect(Collectors.toList()));
  }

  public static Criterion buildCriterion(
      @Nonnull String field, @Nonnull Condition condition, Collection<String> values) {
    return buildCriterion(field, condition, false, values);
  }

  public static Criterion buildCriterion(
      @Nonnull String field, @Nonnull Condition condition, boolean negated, String... values) {
    return buildCriterion(
        field,
        condition,
        negated,
        values == null
            ? Collections.emptyList()
            : Arrays.stream(values).collect(Collectors.toList()));
  }

  public static Criterion buildCriterion(
      @Nonnull String field,
      @Nonnull Condition condition,
      Boolean negated,
      Collection<String> values) {
    Criterion criterion = new Criterion();
    criterion.setField(field);
    criterion.setCondition(condition);
    criterion.setNegated(negated != null ? negated : false);

    List<String> distinctValues =
        values == null ? List.of() : values.stream().distinct().collect(Collectors.toList());
    criterion.setValues(new StringArray(distinctValues));
    return criterion;
  }

  public static ConjunctiveCriterion buildConjunctiveCriterion(Criterion... criteria) {
    ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(
        new CriterionArray(Arrays.stream(criteria).collect(Collectors.toList())));
    return conjunctiveCriterion;
  }
}
