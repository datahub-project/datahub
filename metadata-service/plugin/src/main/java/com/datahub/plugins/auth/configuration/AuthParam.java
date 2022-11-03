package com.datahub.plugins.auth.configuration;

import java.util.Map;
import java.util.Optional;
import lombok.Data;


@Data
public class AuthParam {
  private String className;
  private Optional<String> jarFileName = Optional.empty();
  private Optional<Map<String, Object>> configs = Optional.empty();
}
