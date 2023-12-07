package com.linkedin.datahub.graphql;

import lombok.Data;

@Data
public class VersionedAspectKey {
  private String aspectName;
  private String urn;
  private Long version;

  public VersionedAspectKey(String urn, String aspectName, Long version) {
    this.urn = urn;
    this.version = version;
    this.aspectName = aspectName;
  }
}
