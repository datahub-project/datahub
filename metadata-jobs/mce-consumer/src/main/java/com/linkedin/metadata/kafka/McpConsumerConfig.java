package com.linkedin.metadata.kafka;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.common.GitVersionFactory;
import com.linkedin.metadata.version.GitVersion;
import java.util.HashMap;
import java.util.Map;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@Import(GitVersionFactory.class)
public class McpConsumerConfig {
  private final Map<String, Object> config;
  private final String configJson;

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  public McpConsumerConfig(GitVersion gitVersion) throws JsonProcessingException {
    config = new HashMap<>();
    config.put("noCode", "true");

    Map<String, Object> versionConfig = new HashMap<>();
    versionConfig.put("acryldata/datahub", gitVersion.toConfig());
    config.put("versions", versionConfig);
    configJson = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(config);
  }

  @GetMapping("/config")
  @ResponseBody
  public String getConfig() {
    return configJson;
  }
}
