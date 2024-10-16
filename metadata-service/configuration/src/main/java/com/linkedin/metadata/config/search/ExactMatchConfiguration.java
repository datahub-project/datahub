package com.linkedin.metadata.config.search;

import lombok.Data;

@Data
public class ExactMatchConfiguration {

  private boolean exclusive;
  private boolean withPrefix;
  private float prefixFactor;
  private float exactFactor;
  private float caseSensitivityFactor;
  private boolean enableStructured;
}
