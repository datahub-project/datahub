package io.datahubproject.openapi.v3.models;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@Builder
public class SortCriterion {
  private String field;
  private SortOrder order;
  private MissingValue missingValue;

  @Getter
  public enum MissingValue {

    /** Missing value will be sorted to the top. * */
    FIRST("_first"),

    /** Missing value will be sorted to the end. * */
    LAST("_last");

    private final String value;

    MissingValue(String value) {
      this.value = value;
    }
  }

  public enum SortOrder {
    ASCENDING,
    DESCENDING
  }

  /** Convert this SortCriterion to its counterpart in RecordTemplate. */
  @Nonnull
  public com.linkedin.metadata.query.filter.SortCriterion toRecordTemplate() {
    com.linkedin.metadata.query.filter.SortCriterion sortCriterion =
        new com.linkedin.metadata.query.filter.SortCriterion();
    sortCriterion.setField(this.field);

    // By default, sort in ASCENDING order.
    sortCriterion.setOrder(com.linkedin.metadata.query.filter.SortOrder.ASCENDING);

    if (this.order != null) {
      sortCriterion.setOrder(
          com.linkedin.metadata.query.filter.SortOrder.valueOf(this.order.name()));
    }

    return sortCriterion;
  }
}
