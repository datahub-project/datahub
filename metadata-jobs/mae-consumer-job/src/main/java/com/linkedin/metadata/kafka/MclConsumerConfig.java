package com.linkedin.metadata.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
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
public class MclConsumerConfig {
  private final Map<String, Object> config;
  private final String configJson;

  public MclConsumerConfig(final GitVersion gitVersion, final ObjectMapper objectMapper)
      throws JsonProcessingException {
    config = new HashMap<>();
    config.put("noCode", "true");

    Map<String, Object> versionConfig = new HashMap<>();
    versionConfig.put("acryldata/datahub", gitVersion.toConfig());
    config.put("versions", versionConfig);
    configJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(config);
  }

  @GetMapping("/config")
  @ResponseBody
  public String getConfig() {
    return configJson;
  }
}
