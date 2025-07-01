package io.datahubproject.openapi.v3.models;

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
}
