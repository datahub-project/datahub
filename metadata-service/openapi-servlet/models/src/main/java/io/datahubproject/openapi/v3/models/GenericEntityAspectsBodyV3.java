package io.datahubproject.openapi.v3.models;

import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@Builder
public class GenericEntityAspectsBodyV3 {
  @Nullable private Set<String> entities;
  @Nullable private Set<String> aspects;
  @Nullable private Filter filter;

  /** This will be ignored if sorting criterion is also specified as param on URL path. */
  @Nullable private List<SortCriterion> sortCriteria;
}
