package com.linkedin.metadata.timeline;

import lombok.Builder;
import lombok.Getter;


@Builder
public class SemanticVersion {
  @Getter
  private int majorVersion;
  @Getter
  private int minorVersion;
  @Getter
  private int patchVersion;
  @Getter
  private String qualifier;

  public String toString() {
    return String.format(String.format("%d.%d.%d-%s", majorVersion, minorVersion, patchVersion, qualifier));
  }
}
