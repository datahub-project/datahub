package io.datahubproject.openapi.v3.models;

import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@Builder
public class Filter {
  /**
   * A list criterion for the filter. OR operation to combine - ConjunctiveCriterion OR
   * ConjunctiveCriterion
   */
  private List<ConjunctiveCriterion> and;

  /** Convert this filter to its counterpart in RecordTemplate. */
  @Nullable
  public com.linkedin.metadata.query.filter.Filter toRecordTemplate() {

    if (and == null) return null;

    com.linkedin.metadata.query.filter.Filter filter =
        new com.linkedin.metadata.query.filter.Filter();
    ConjunctiveCriterionArray conjunctiveCriterionArray = new ConjunctiveCriterionArray();

    for (ConjunctiveCriterion criterion : and) {
      conjunctiveCriterionArray.add(criterion.toRecordTemplate());
    }

    filter.setOr(conjunctiveCriterionArray);

    return filter;
  }
}
