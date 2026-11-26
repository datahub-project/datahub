package com.linkedin.metadata.entity;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import org.testng.annotations.Test;

public class GraphReferenceOverLimitTest {
  @Test
  public void parseIgnoresCaseAndWhitespace() {
    assertEquals(GraphReferenceOverLimit.parse("skip"), GraphReferenceOverLimit.SKIP);
    assertEquals(GraphReferenceOverLimit.parse(" Refuse "), GraphReferenceOverLimit.REFUSE);
  }

  @Test
  public void parseRejectsAnUnknownMode() {
    expectThrows(IllegalArgumentException.class, () -> GraphReferenceOverLimit.parse("ignore"));
  }
}
