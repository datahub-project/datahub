package com.linkedin.metadata.config;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class UrlValidationConfig {
  @Builder.Default private boolean enabled = true;
  @Builder.Default private boolean allowHttp = false;
  @Builder.Default private List<String> extraDenyHosts = List.of();
}
