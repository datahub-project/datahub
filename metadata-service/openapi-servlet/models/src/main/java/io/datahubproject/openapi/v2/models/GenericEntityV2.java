package io.datahubproject.openapi.v2.models;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.openapi.models.GenericEntity;
import io.swagger.v3.oas.annotations.media.Schema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
public class GenericEntityV2 implements GenericEntity<GenericAspectV2> {
  @JsonProperty("urn")
  @Schema(description = "Urn of the entity")
  private String urn;

  @JsonProperty("aspects")
  @Schema(description = "Map of aspect name to aspect")
  private Map<String, GenericAspectV2> aspects;

  public static class GenericEntityV2Builder {

    public GenericEntityV2 build(
        ObjectMapper objectMapper, Map<String, Pair<RecordTemplate, SystemMetadata>> aspects) {
      return build(objectMapper, aspects, false);
    }

    public GenericEntityV2 build(
        ObjectMapper objectMapper,
        Map<String, Pair<RecordTemplate, SystemMetadata>> aspects,
        boolean isAsyncAlternateValidation) {
      Map<String, GenericAspectV2> jsonObjectMap =
          aspects.entrySet().stream()
              .map(
                  e -> {
                    try {
                      Map<String, JsonNode> valueMap =
                          Map.of(
                              "value",
                              objectMapper.readTree(
                                  RecordUtils.toJsonString(e.getValue().getFirst())
                                      .getBytes(StandardCharsets.UTF_8)));

                      Object aspectValue =
                          isAsyncAlternateValidation
                              ? valueMap.get("value").get("value")
                              : valueMap.get("value");

                      if (e.getValue().getSecond() != null) {
                        return Map.entry(
                            e.getKey(),
                            new GenericAspectV2(
                                Map.of(
                                    "systemMetadata",
                                    e.getValue().getSecond(),
                                    "value",
                                    aspectValue)));
                      } else {
                        return Map.entry(
                            e.getKey(), new GenericAspectV2(Map.of("value", aspectValue)));
                      }
                    } catch (IOException ex) {
                      throw new RuntimeException(ex);
                    }
                  })
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      return new GenericEntityV2(urn, jsonObjectMap);
    }
  }
}
