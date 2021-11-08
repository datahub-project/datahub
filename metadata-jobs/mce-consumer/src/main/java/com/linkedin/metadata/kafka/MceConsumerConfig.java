package com.linkedin.metadata.kafka;

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
public class MceConsumerConfig {
  private final Map<String, String> config;

  public MceConsumerConfig(GitVersion gitVersion) {
    config = new HashMap<>();
    config.put("noCode", "true");

    config.put("version", gitVersion.getVersion());
    config.put("commit", gitVersion.getCommitId());
  }

  @GetMapping("/config")
  @ResponseBody
  public Map<String, String> getConfig() {
    return config;
  }
}
