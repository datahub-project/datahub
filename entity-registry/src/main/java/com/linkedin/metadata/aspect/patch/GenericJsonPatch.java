package com.linkedin.metadata.aspect.patch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonpatch.JsonPatch;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GenericJsonPatch {
  @Nullable private Map<String, List<String>> arrayPrimaryKeys;

  @Nonnull private JsonNode patch;

  @Nonnull
  public Map<String, List<String>> getArrayPrimaryKeys() {
    return arrayPrimaryKeys == null ? Collections.emptyMap() : arrayPrimaryKeys;
  }

  @JsonIgnore
  public JsonPatch getJsonPatch() throws IOException {
    return JsonPatch.fromJson(patch);
  }
}
