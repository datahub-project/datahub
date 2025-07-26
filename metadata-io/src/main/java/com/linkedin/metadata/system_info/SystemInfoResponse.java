package com.linkedin.metadata.system_info;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SystemInfoResponse {
  private SpringComponentsInfo springComponents;
  private SystemPropertiesInfo properties;
}
