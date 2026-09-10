package io.datahubproject.openapi.v3.models;

import java.util.List;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@Builder
public class ScrollLineageRequestBody {
  @Nullable private List<String> urns;
}
