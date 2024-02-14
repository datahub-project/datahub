package com.datahub.authorization.config;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class SearchAuthorizationConfiguration {
  private boolean enabled;
}
