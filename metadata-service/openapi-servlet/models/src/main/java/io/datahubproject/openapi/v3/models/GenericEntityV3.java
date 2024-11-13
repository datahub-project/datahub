package io.datahubproject.openapi.v3.models;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
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
public class GenericEntityV3 extends LinkedHashMap<String, Object>
    implements GenericEntity<GenericAspectV3> {

  public GenericEntityV3(Map<? extends String, ?> m) {
    super(m);
  }

  public String getUrn() {
    return (String) get("urn");
  }

  @Override
  public Map<String, GenericAspectV3> getAspects() {
    return this.entrySet().stream()
        .filter(entry -> !"urn".equals(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> (GenericAspectV3) entry.getValue()));
  }

  public static class GenericEntityV3Builder {

    public GenericEntityV3 build(
        ObjectMapper objectMapper, @Nonnull Urn urn, Map<String, AspectItem> aspects) {
      Map<String, GenericAspectV3> jsonObjectMap =
          aspects.entrySet().stream()
              .map(
                  entry -> {
                    try {
                      String aspectName = entry.getKey();
                      Map<String, Object> aspectValueMap =
                          objectMapper.readValue(
                              RecordUtils.toJsonString(entry.getValue().getAspect())
                                  .getBytes(StandardCharsets.UTF_8),
                              new TypeReference<>() {});

                      Map<String, Object> systemMetadata =
                          entry.getValue().getSystemMetadata() != null
                              ? objectMapper.convertValue(
                                  entry.getValue().getSystemMetadata(), new TypeReference<>() {})
                              : null;
                      Map<String, Object> auditStamp =
                          entry.getValue().getAuditStamp() != null
                              ? objectMapper.convertValue(
                                  entry.getValue().getAuditStamp().data(), new TypeReference<>() {})
                              : null;

                      return Map.entry(
                          aspectName,
                          GenericAspectV3.builder()
                              .value(aspectValueMap)
                              .systemMetadata(systemMetadata)
                              .auditStamp(auditStamp)
                              .build());
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
