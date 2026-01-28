package com.linkedin.metadata.system_info;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SpringComponentsInfo {
  private ComponentInfo gms;
  private ComponentInfo maeConsumer;
  private ComponentInfo mceConsumer;
}
