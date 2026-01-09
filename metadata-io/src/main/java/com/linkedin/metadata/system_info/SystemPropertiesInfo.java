package com.linkedin.metadata.system_info;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SystemPropertiesInfo {
  private Map<String, PropertyInfo> properties;
  private List<PropertySourceInfo> propertySources;
  private int totalProperties;
  private int redactedProperties;
}
