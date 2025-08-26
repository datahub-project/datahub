package com.linkedin.metadata.config;

import java.util.Collections;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class EntityServiceConfiguration {
  public static EntityServiceConfiguration EMPTY =
      new EntityServiceConfiguration().setApplyMclSyncForSources(Collections.emptyMap());

  public Map<String, Boolean> applyMclSyncForSources;
}
