package io.datahubproject.openapi.v2.models;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GenericEntity {
  private String urn;
  private Map<String, Object> aspects;

  public static class GenericEntityBuilder {

    public GenericEntity build(
        ObjectMapper objectMapper, Map<String, Pair<RecordTemplate, SystemMetadata>> aspects) {
      Map<String, Object> jsonObjectMap =
          aspects.entrySet().stream()
              .map(
                  e -> {
                    try {
                      Map<String, Object> valueMap =
                          Map.of(
                              "value",
                              objectMapper.readTree(
                                  RecordUtils.toJsonString(e.getValue().getFirst())
                                      .getBytes(StandardCharsets.UTF_8)));

                      if (e.getValue().getSecond() != null) {
                        return Map.entry(
                            e.getKey(),
                            Map.of(
                                "systemMetadata", e.getValue().getSecond(),
                                "value", valueMap.get("value")));
                      } else {
                        return Map.entry(e.getKey(), Map.of("value", valueMap.get("value")));
                      }
                    } catch (IOException ex) {
                      throw new RuntimeException(ex);
                    }
                  })
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      return new GenericEntity(urn, jsonObjectMap);
    }
  }
}
