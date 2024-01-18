package com.datahub.authorization;

import com.linkedin.data.template.StringArray;
import com.linkedin.policy.PolicyMatchCondition;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchCriterionArray;
import com.linkedin.policy.PolicyMatchFilter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class FilterUtils {

  public static final PolicyMatchFilter EMPTY_FILTER =
      new PolicyMatchFilter().setCriteria(new PolicyMatchCriterionArray());

  private FilterUtils() {}

  /** Creates new PolicyMatchCriterion with field and value, using EQUAL PolicyMatchCondition. */
  @Nonnull
  public static PolicyMatchCriterion newCriterion(
      @Nonnull EntityFieldType field, @Nonnull List<String> values) {
    return newCriterion(field, values, PolicyMatchCondition.EQUALS);
  }

  /** Creates new PolicyMatchCriterion with field, value and PolicyMatchCondition. */
  @Nonnull
  public static PolicyMatchCriterion newCriterion(
      @Nonnull EntityFieldType field,
      @Nonnull List<String> values,
      @Nonnull PolicyMatchCondition policyMatchCondition) {
    return new PolicyMatchCriterion()
        .setField(field.name())
        .setValues(new StringArray(values))
        .setCondition(policyMatchCondition);
  }

  /**
   * Creates new PolicyMatchFilter from a map of Criteria by removing null-valued Criteria and using
   * EQUAL PolicyMatchCondition (default).
   */
  @Nonnull
  public static PolicyMatchFilter newFilter(@Nullable Map<EntityFieldType, List<String>> params) {
    if (params == null) {
      return EMPTY_FILTER;
    }
    PolicyMatchCriterionArray criteria =
        params.entrySet().stream()
            .filter(e -> Objects.nonNull(e.getValue()))
            .map(e -> newCriterion(e.getKey(), e.getValue()))
            .collect(Collectors.toCollection(PolicyMatchCriterionArray::new));
    return new PolicyMatchFilter().setCriteria(criteria);
  }

  /**
   * Creates new PolicyMatchFilter from a single PolicyMatchCriterion with EQUAL
   * PolicyMatchCondition (default).
   */
  @Nonnull
  public static PolicyMatchFilter newFilter(
      @Nonnull EntityFieldType field, @Nonnull List<String> values) {
    return newFilter(Collections.singletonMap(field, values));
  }
}
