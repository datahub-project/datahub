package com.linkedin.metadata.config;

import lombok.Data;


@Data
public class BuildIndicesConfiguration {

  private String initialBackOffMs;
  private String maxBackOffs;
  private String backOffFactor;
  private boolean waitForBuildIndices;
  private boolean cloneIndices;

}
