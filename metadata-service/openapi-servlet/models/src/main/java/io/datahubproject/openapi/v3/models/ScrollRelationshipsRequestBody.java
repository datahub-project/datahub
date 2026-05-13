package io.datahubproject.openapi.v3.models;

import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@Builder
public class ScrollRelationshipsRequestBody {
  @Nullable private Filter sourceFilter;
  @Nullable private Filter destinationFilter;
  @Nullable private Filter edgeFilter;
}
