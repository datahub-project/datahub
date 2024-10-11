package com.linkedin.metadata.test.query;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import lombok.Builder;
import lombok.Getter;

@Getter
@JsonDeserialize(builder = TestQuery.TestQueryBuilder.class)
public class TestQuery {
  private final String query;
  private final List<String> queryParts;

  public TestQuery(List<String> queryParts) {
    this.queryParts = queryParts;
    this.query = String.join(".", queryParts);
  }

  public TestQuery(String query) {
    this.query = query;
    this.queryParts = Arrays.asList(query.split("\\."));
  }

  @Builder
  public TestQuery(String query, List<String> queryParts) {
    this.query = query;
    this.queryParts = queryParts;
  }

  @Override
  public String toString() {
    return query;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TestQuery that = (TestQuery) o;
    return Objects.equals(query, that.query);
  }

  @Override
  public int hashCode() {
    return Objects.hash(query);
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class TestQueryBuilder {}
}
