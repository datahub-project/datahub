package com.linkedin.metadata.utils.elasticsearch.shim;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class KnnSearchRequest {
  private final String indexName;
  private final String vectorField;
  private final float[] queryVector;
  private final int k;
  private final int numCandidates;
  private final List<String> fieldsToFetch;
  private final Map<String, Object> filter;
  private final boolean ignoreUnavailable;

  private KnnSearchRequest(Builder b) {
    this.indexName = b.indexName;
    this.vectorField = b.vectorField;
    this.queryVector = b.queryVector.clone();
    this.k = b.k;
    this.numCandidates = b.numCandidates > 0 ? b.numCandidates : Math.max(b.k * 10, 10);
    this.fieldsToFetch = b.fieldsToFetch != null ? List.copyOf(b.fieldsToFetch) : List.of();
    // Map.copyOf rejects null values; use an unmodifiable HashMap copy to preserve null-valued
    // filter clauses (e.g. "missing" semantics in ES/OS filter DSL).
    this.filter =
        b.filter != null ? Collections.unmodifiableMap(new HashMap<>(b.filter)) : Map.of();
    this.ignoreUnavailable = b.ignoreUnavailable;
  }

  @Nonnull
  public String indexName() {
    return indexName;
  }

  @Nonnull
  public String vectorField() {
    return vectorField;
  }

  @Nonnull
  public float[] queryVector() {
    return queryVector.clone();
  }

  public int k() {
    return k;
  }

  public int numCandidates() {
    return numCandidates;
  }

  @Nonnull
  public List<String> fieldsToFetch() {
    return fieldsToFetch;
  }

  @Nonnull
  public Optional<Map<String, Object>> filter() {
    return filter.isEmpty() ? Optional.empty() : Optional.of(filter);
  }

  public boolean ignoreUnavailable() {
    return ignoreUnavailable;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String indexName;
    private String vectorField;
    private float[] queryVector;
    private int k;
    private int numCandidates;
    private List<String> fieldsToFetch;
    private Map<String, Object> filter;
    private boolean ignoreUnavailable = true;

    public Builder indexName(@Nonnull String v) {
      this.indexName = v;
      return this;
    }

    public Builder vectorField(@Nonnull String v) {
      this.vectorField = v;
      return this;
    }

    public Builder queryVector(@Nonnull float[] v) {
      this.queryVector = v;
      return this;
    }

    public Builder k(int v) {
      this.k = v;
      return this;
    }

    public Builder numCandidates(int v) {
      this.numCandidates = v;
      return this;
    }

    public Builder fieldsToFetch(@Nullable List<String> v) {
      this.fieldsToFetch = v;
      return this;
    }

    public Builder filter(@Nullable Map<String, Object> v) {
      this.filter = v;
      return this;
    }

    public Builder ignoreUnavailable(boolean v) {
      this.ignoreUnavailable = v;
      return this;
    }

    public KnnSearchRequest build() {
      if (indexName == null || vectorField == null) {
        throw new IllegalStateException("indexName and vectorField are required");
      }
      if (queryVector == null || queryVector.length == 0) {
        throw new IllegalStateException("queryVector is required and non-empty");
      }
      if (k < 1) {
        throw new IllegalStateException("k must be >= 1");
      }
      return new KnnSearchRequest(this);
    }
  }
}
