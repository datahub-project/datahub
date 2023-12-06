package io.datahubproject.openapi.v3.models;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.template.RecordTemplate;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@Builder
public class GenericEntity {
    private String urn;
    private Map<String, Object> aspects;

    public static class GenericEntityBuilder {
        public GenericEntity build(ObjectMapper objectMapper, Map<String, RecordTemplate> aspects) {
            Map<String, Object> jsonObjectMap = aspects.entrySet().stream()
                    .map(e -> {
                        try {
                            return Map.entry(e.getKey(),
                                    objectMapper.readTree(RecordUtils.toJsonString(e.getValue()).getBytes(StandardCharsets.UTF_8)));
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                    }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            return new GenericEntity(urn, jsonObjectMap);
        }
    }
}
