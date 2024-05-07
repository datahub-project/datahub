package com.linkedin.metadata.aspect.patch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonPatch;
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

  @Nonnull private List<PatchOp> patch;

  @Nonnull
  public Map<String, List<String>> getArrayPrimaryKeys() {
    return arrayPrimaryKeys == null ? Collections.emptyMap() : arrayPrimaryKeys;
  }

  @JsonIgnore
  public JsonPatch getJsonPatch() {
    JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
    patch.forEach(op -> arrayBuilder.add(Json.createObjectBuilder(op.toMap())));
    return Json.createPatch(arrayBuilder.build());
  }

  @Data
  @NoArgsConstructor
  public static class PatchOp {
    @Nonnull private String op;
    @Nonnull private String path;
    @Nullable private Object value;

    public Map<String, ?> toMap() {
      if (value != null) {
        return Map.of("op", op, "path", path, "value", value);
      } else {
        return Map.of("op", op, "path", path);
      }
    }
  }
}
