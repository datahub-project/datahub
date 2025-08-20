package io.datahubproject.openapi.v3.models;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@Builder
public class SortCriterion {
  private String field;
  private SortOrder order;

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
    sortCriterion.setOrder(com.linkedin.metadata.query.filter.SortOrder.valueOf(this.order.name()));

    return sortCriterion;
  }
}
