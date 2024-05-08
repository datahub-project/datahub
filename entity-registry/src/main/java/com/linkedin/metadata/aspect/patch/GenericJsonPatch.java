package com.linkedin.metadata.aspect.patch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.linkedin.util.Pair;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonPatch;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
        return Stream.of(Pair.of("op", op), Pair.of("path", path), Pair.of("value", value))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
      } else {
        return Stream.of(Pair.of("op", op), Pair.of("path", path))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
      }
    }
  }
}
