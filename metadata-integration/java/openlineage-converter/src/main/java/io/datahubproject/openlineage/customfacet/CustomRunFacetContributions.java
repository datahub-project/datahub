package io.datahubproject.openlineage.customfacet;

import com.linkedin.common.GlobalTags;
import com.linkedin.data.template.StringMap;

public record CustomRunFacetContributions(
    StringMap flowProperties, StringMap jobProperties, GlobalTags flowTags) {
  public CustomRunFacetContributions {
    flowProperties = flowProperties == null ? new StringMap() : flowProperties;
    jobProperties = jobProperties == null ? new StringMap() : jobProperties;
  }

  public static CustomRunFacetContributions empty() {
    return new CustomRunFacetContributions(new StringMap(), new StringMap(), null);
  }
}
