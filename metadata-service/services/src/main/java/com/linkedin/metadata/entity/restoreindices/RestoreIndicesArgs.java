package com.linkedin.metadata.entity.restoreindices;

import lombok.Data;

@Data
public class RestoreIndicesArgs implements Cloneable {
  public int start = 0;
  public int batchSize = 10;
  public int numThreads = 1;
  public long batchDelayMs = 1;
  public String aspectName;
  public String urn;
  public String urnLike;

  @Override
  public RestoreIndicesArgs clone() {
    try {
      RestoreIndicesArgs clone = (RestoreIndicesArgs) super.clone();
      // TODO: copy mutable state here, so the clone can't change the internals of the original
      return clone;
    } catch (CloneNotSupportedException e) {
      throw new AssertionError();
    }
  }

  public RestoreIndicesArgs setAspectName(String aspectName) {
    this.aspectName = aspectName;
    return this;
  }

  public RestoreIndicesArgs setUrnLike(String urnLike) {
    this.urnLike = urnLike;
    return this;
  }

  public RestoreIndicesArgs setUrn(String urn) {
    this.urn = urn;
    return this;
  }

  public RestoreIndicesArgs setStart(Integer start) {
    if (start != null) {
      this.start = start;
    }
    return this;
  }

  public RestoreIndicesArgs setBatchSize(Integer batchSize) {
    if (batchSize != null) {
      this.batchSize = batchSize;
    }
    return this;
  }
}
