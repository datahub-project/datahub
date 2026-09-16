package com.linkedin.metadata.system_info;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PropertyInfo {
  private String key;
  private Object value;
  private String source;
  private String sourceType;
  private String resolvedValue;
}
