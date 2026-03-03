package com.linkedin.metadata.system_info;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PropertySourceInfo {
  private String name;
  private String type;
  private int propertyCount;
}
