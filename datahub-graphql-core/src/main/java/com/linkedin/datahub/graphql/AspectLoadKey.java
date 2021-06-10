package com.linkedin.datahub.graphql;

import lombok.Data;

@Data
public class AspectLoadKey {
  private String aspectName;
  private String urn;
  private Long version;

  public AspectLoadKey(String _urn, String _aspectName,  Long _version) {
    urn = _urn;
    version = _version;
    aspectName = _aspectName;
  }
}
