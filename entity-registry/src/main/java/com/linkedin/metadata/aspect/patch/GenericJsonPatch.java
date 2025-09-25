package com.linkedin.metadata.aspect.patch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonPatch;
import java.util.Collections;
import java.util.HashMap;
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
  public static final String ARRAY_PRIMARY_KEYS_FIELD = "arrayPrimaryKeys";
  public static final String PATCH_FIELD = "patch";

  @Nullable private Map<String, List<String>> arrayPrimaryKeys;

  @Nonnull private List<PatchOp> patch;

  /** Bypass template engine even if a patch template exists */
  private boolean forceGenericPatch;

  @Nonnull
  public Map<String, List<String>> getArrayPrimaryKeys() {
    return arrayPrimaryKeys == null ? Collections.emptyMap() : arrayPrimaryKeys;
  }

  @JsonIgnore
  public JsonPatch getJsonPatch() {
    JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
    patch.forEach(
        op -> {
          Map<String, ?> opMap = op.toMap();
          JsonObjectBuilder opBuilder = Json.createObjectBuilder();

          // Handle each field, converting null values to empty strings for ADD operations
          opMap.forEach(
              (key, value) -> {
                if ("value".equals(key) && "add".equals(op.getOp()) && value == null) {
                  opBuilder.add(key, ""); // Convert null to empty string for ADD operations
                } else if (value == null) {
                  opBuilder.addNull(key);
                } else if (value instanceof String) {
                  opBuilder.add(key, (String) value);
                } else if (value instanceof Integer) {
                  opBuilder.add(key, (Integer) value);
                } else if (value instanceof Long) {
                  opBuilder.add(key, (Long) value);
                } else if (value instanceof Boolean) {
                  opBuilder.add(key, (Boolean) value);
                } else if (value instanceof Double) {
                  opBuilder.add(key, (Double) value);
                } else {
                  opBuilder.add(key, value.toString());
                }
              });

          arrayBuilder.add(opBuilder);
        });
    return Json.createPatch(arrayBuilder.build());
  }

  @Data
  @NoArgsConstructor
  public static class PatchOp {
    @Nonnull private String op;
    @Nonnull private String path;
    @Nullable private Object value;

    public Map<String, ?> toMap() {
      // For ADD operations, always include value field (Jakarta JSON Patch requirement)
      if ("add".equals(op) || value != null) {
        Map<String, Object> map = new HashMap<>();
        map.put("op", op);
        map.put("path", path);
        map.put("value", value); // Can be null for ADD operations
        return map;
      } else {
        Map<String, Object> map = new HashMap<>();
        map.put("op", op);
        map.put("path", path);
        return map;
      }
    }
  }
}
