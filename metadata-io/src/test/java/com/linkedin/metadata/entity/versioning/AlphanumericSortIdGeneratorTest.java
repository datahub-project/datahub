package com.linkedin.metadata.entity.versioning;

import static com.linkedin.metadata.Constants.INITIAL_VERSION_SORT_ID;
import static org.testng.Assert.*;

import org.testng.annotations.Test;

public class AlphanumericSortIdGeneratorTest {

  @Test
  public void testBasicIncrement() {
    assertEquals(AlphanumericSortIdGenerator.increment(INITIAL_VERSION_SORT_ID), "AAAAAAAB");
    assertEquals(AlphanumericSortIdGenerator.increment("AAAAAAAB"), "AAAAAAAC");
  }

  @Test
  public void testCarryOver() {
    assertEquals(AlphanumericSortIdGenerator.increment("AAAAAAAZ"), "AAAAAABA");
    assertEquals(AlphanumericSortIdGenerator.increment("AAAAAZZZ"), "AAAABAAA");
  }

  @Test
  public void testWrapAround() {
    assertEquals(AlphanumericSortIdGenerator.increment("ZZZZZZZZ"), INITIAL_VERSION_SORT_ID);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidLength() {
    AlphanumericSortIdGenerator.increment("AAA");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidCharacters() {
    AlphanumericSortIdGenerator.increment("AAAA$AAA");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNullInput() {
    AlphanumericSortIdGenerator.increment(null);
  }

  @Test
  public void testSequence() {
    String id = "AAAAAAAA";
    id = AlphanumericSortIdGenerator.increment(id);
    assertEquals(id, "AAAAAAAB");
    id = AlphanumericSortIdGenerator.increment(id);
    assertEquals(id, "AAAAAAAC");
    id = AlphanumericSortIdGenerator.increment(id);
    assertEquals(id, "AAAAAAAD");
  }

  @Test
  public void testLowerBoundary() {
    assertEquals(AlphanumericSortIdGenerator.increment(INITIAL_VERSION_SORT_ID), "AAAAAAAB");
  }

  @Test
  public void testUpperBoundary() {
    assertEquals(AlphanumericSortIdGenerator.increment("ZZZZZZZZ"), "AAAAAAAA");
  }
}
