package io.datahubproject.openapi.v3.models;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.openapi.models.GenericEntity;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@AllArgsConstructor
public class GenericEntityV3 extends LinkedHashMap<String, Object> implements GenericEntity {

  public GenericEntityV3(Map<? extends String, ?> m) {
    super(m);
  }

  @Override
  public Map<String, Object> getAspects() {
    return this;
  }

  public static class GenericEntityV3Builder {

    public GenericEntityV3 build(
        ObjectMapper objectMapper,
        @Nonnull Urn urn,
        Map<String, Pair<RecordTemplate, SystemMetadata>> aspects) {
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

      GenericEntityV3 genericEntityV3 = new GenericEntityV3();
      genericEntityV3.put("urn", urn.toString());
      genericEntityV3.putAll(jsonObjectMap);
      return genericEntityV3;
    }
  }
}
