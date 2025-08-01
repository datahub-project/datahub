package com.linkedin.metadata.system_info;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ComponentInfo {
  private String name;
  private ComponentStatus status;
  private String version;
  private Map<String, Object> properties;
  private String errorMessage;
}
