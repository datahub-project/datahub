package io.datahubproject.openapi.v3.models;

import com.linkedin.data.template.StringArray;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@Builder
public class Criterion {
  /** The name of the field that the criterion refers to */
  private String field;

  /**
   * Values. one of which the intended field should match Note, if values is set, the above "value"
   * field will be ignored
   */
  private List<String> values;

  /** The condition for the criterion, e.g. EQUAL, START_WITH */
  private Condition condition;

  /** Whether the condition should be negated */
  private Boolean negated;

  public enum Condition {
    EQUAL,
    STARTS_WITH,
    ENDS_WITH,
    EXISTS,
    IN,
    CONTAIN,
    GREATER_THAN,
    LESS_THAN,
  }

  /** Convert this criterion to its counterpart in RecordTemplate. */
  @Nullable
  public com.linkedin.metadata.query.filter.Criterion toRecordTemplate() {
    com.linkedin.metadata.query.filter.Criterion criterion =
        new com.linkedin.metadata.query.filter.Criterion();

    criterion.setField(this.field);

    if (this.values != null) {
      criterion.setValues(new StringArray(this.values));
    }

    // By default, the condition is EQUAL.
    criterion.setCondition(com.linkedin.metadata.query.filter.Condition.EQUAL);

    if (this.condition != null) {
      criterion.setCondition(
          com.linkedin.metadata.query.filter.Condition.valueOf(this.condition.name()));
    }

    if (this.negated != null) {
      criterion.setNegated(this.negated);
    }

    return criterion;
  }
}
