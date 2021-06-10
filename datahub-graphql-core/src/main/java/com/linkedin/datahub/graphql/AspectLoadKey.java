package com.linkedin.datahub.graphql;

import lombok.Data;

@Data
public class AspectLoadKey {
  private String aspectName;
  private String urn;
  private Long version;

  public AspectLoadKey(String urn, String aspectName,  Long version) {
    this.urn = urn;
    this.version = version;
    this.aspectName = aspectName;
  }
}
