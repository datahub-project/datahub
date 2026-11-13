package com.linkedin.metadata.utils.elasticsearch.shim;

import javax.annotation.Nonnull;

public final class SemanticIndexSpec {
  private final String indexName;
  private final String modelKey;
  private final int vectorDimension;
  private final String similarity;
  private final int hnswM;
  private final int hnswEfConstruction;
  private final String knnEngine;

  private SemanticIndexSpec(Builder b) {
    this.indexName = b.indexName;
    this.modelKey = b.modelKey;
    this.vectorDimension = b.vectorDimension;
    this.similarity = b.similarity;
    this.hnswM = b.hnswM;
    this.hnswEfConstruction = b.hnswEfConstruction;
    this.knnEngine = b.knnEngine;
  }

  @Nonnull
  public String indexName() {
    return indexName;
  }

  @Nonnull
  public String modelKey() {
    return modelKey;
  }

  public int vectorDimension() {
    return vectorDimension;
  }

  @Nonnull
  public String similarity() {
    return similarity;
  }

  public int hnswM() {
    return hnswM;
  }

  public int hnswEfConstruction() {
    return hnswEfConstruction;
  }

  @Nonnull
  public String knnEngine() {
    return knnEngine;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String indexName;
    private String modelKey;
    private int vectorDimension;
    private String similarity = "cosine";
    private int hnswM = 16;
    private int hnswEfConstruction = 128;
    private String knnEngine = "faiss";

    public Builder indexName(@Nonnull String v) {
      this.indexName = v;
      return this;
    }

    public Builder modelKey(@Nonnull String v) {
      this.modelKey = v;
      return this;
    }

    public Builder vectorDimension(int v) {
      this.vectorDimension = v;
      return this;
    }

    public Builder similarity(@Nonnull String v) {
      this.similarity = v;
      return this;
    }

    public Builder hnswM(int v) {
      this.hnswM = v;
      return this;
    }

    public Builder hnswEfConstruction(int v) {
      this.hnswEfConstruction = v;
      return this;
    }

    public Builder knnEngine(@Nonnull String v) {
      this.knnEngine = v;
      return this;
    }

    public SemanticIndexSpec build() {
      if (indexName == null || modelKey == null) {
        throw new IllegalStateException("indexName and modelKey required");
      }
      if (vectorDimension < 1) {
        throw new IllegalStateException("vectorDimension must be >= 1");
      }
      return new SemanticIndexSpec(this);
    }
  }
}
