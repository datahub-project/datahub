package com.linkedin.metadata.test.query;

import java.util.Collections;
import java.util.List;
import lombok.Value;


@Value
public class TestQueryResponse {
  List<String> values;

  private static final TestQueryResponse EMPTY = new TestQueryResponse(Collections.emptyList());

  public static TestQueryResponse empty() {
    return EMPTY;
  }
}
