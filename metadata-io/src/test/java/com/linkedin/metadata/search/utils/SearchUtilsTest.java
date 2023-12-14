package com.linkedin.metadata.search.utils;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.query.SearchFlags;
import java.util.Set;
import org.testng.annotations.Test;

public class SearchUtilsTest {

  @Test
  public void testApplyDefaultSearchFlags() {
    SearchFlags defaultFlags =
        new SearchFlags()
            .setFulltext(true)
            .setSkipCache(true)
            .setSkipAggregates(true)
            .setMaxAggValues(1)
            .setSkipHighlighting(true);

    assertEquals(
        SearchUtils.applyDefaultSearchFlags(null, "not empty", defaultFlags),
        defaultFlags,
        "Expected all default values");

    assertEquals(
        SearchUtils.applyDefaultSearchFlags(
            new SearchFlags()
                .setFulltext(false)
                .setSkipCache(false)
                .setSkipAggregates(false)
                .setMaxAggValues(2)
                .setSkipHighlighting(false),
            "not empty",
            defaultFlags),
        new SearchFlags()
            .setFulltext(false)
            .setSkipAggregates(false)
            .setSkipCache(false)
            .setMaxAggValues(2)
            .setSkipHighlighting(false),
        "Expected no default values");

    assertEquals(
        SearchUtils.applyDefaultSearchFlags(
            new SearchFlags()
                .setFulltext(false)
                .setSkipCache(false)
                .setSkipAggregates(false)
                .setMaxAggValues(2)
                .setSkipHighlighting(false),
            null,
            defaultFlags),
        new SearchFlags()
            .setFulltext(false)
            .setSkipAggregates(false)
            .setSkipCache(false)
            .setMaxAggValues(2)
            .setSkipHighlighting(true),
        "Expected skip highlight due to query null query");
    for (String query : Set.of("*", "")) {
      assertEquals(
          SearchUtils.applyDefaultSearchFlags(
              new SearchFlags()
                  .setFulltext(false)
                  .setSkipCache(false)
                  .setSkipAggregates(false)
                  .setMaxAggValues(2)
                  .setSkipHighlighting(false),
              query,
              defaultFlags),
          new SearchFlags()
              .setFulltext(false)
              .setSkipAggregates(false)
              .setSkipCache(false)
              .setMaxAggValues(2)
              .setSkipHighlighting(true),
          String.format("Expected skip highlight due to query string `%s`", query));
    }

    assertEquals(
        SearchUtils.applyDefaultSearchFlags(
            new SearchFlags().setFulltext(false), "not empty", defaultFlags),
        new SearchFlags()
            .setFulltext(false)
            .setSkipAggregates(true)
            .setSkipCache(true)
            .setMaxAggValues(1)
            .setSkipHighlighting(true),
        "Expected all default values except fulltext");
    assertEquals(
        SearchUtils.applyDefaultSearchFlags(
            new SearchFlags().setSkipCache(false), "not empty", defaultFlags),
        new SearchFlags()
            .setFulltext(true)
            .setSkipAggregates(true)
            .setSkipCache(false)
            .setMaxAggValues(1)
            .setSkipHighlighting(true),
        "Expected all default values except skipCache");
    assertEquals(
        SearchUtils.applyDefaultSearchFlags(
            new SearchFlags().setSkipAggregates(false), "not empty", defaultFlags),
        new SearchFlags()
            .setFulltext(true)
            .setSkipAggregates(false)
            .setSkipCache(true)
            .setMaxAggValues(1)
            .setSkipHighlighting(true),
        "Expected all default values except skipAggregates");
    assertEquals(
        SearchUtils.applyDefaultSearchFlags(
            new SearchFlags().setMaxAggValues(2), "not empty", defaultFlags),
        new SearchFlags()
            .setFulltext(true)
            .setSkipAggregates(true)
            .setSkipCache(true)
            .setMaxAggValues(2)
            .setSkipHighlighting(true),
        "Expected all default values except maxAggValues");
    assertEquals(
        SearchUtils.applyDefaultSearchFlags(
            new SearchFlags().setSkipHighlighting(false), "not empty", defaultFlags),
        new SearchFlags()
            .setFulltext(true)
            .setSkipAggregates(true)
            .setSkipCache(true)
            .setMaxAggValues(1)
            .setSkipHighlighting(false),
        "Expected all default values except skipHighlighting");
  }

  @Test
  public void testImmutableDefaults() throws CloneNotSupportedException {
    SearchFlags defaultFlags =
        new SearchFlags()
            .setFulltext(true)
            .setSkipCache(true)
            .setSkipAggregates(true)
            .setMaxAggValues(1)
            .setSkipHighlighting(true);
    SearchFlags copyFlags = defaultFlags.copy();

    assertEquals(
        SearchUtils.applyDefaultSearchFlags(
            new SearchFlags()
                .setFulltext(false)
                .setSkipCache(false)
                .setSkipAggregates(false)
                .setMaxAggValues(2)
                .setSkipHighlighting(false),
            "not empty",
            defaultFlags),
        new SearchFlags()
            .setFulltext(false)
            .setSkipAggregates(false)
            .setSkipCache(false)
            .setMaxAggValues(2)
            .setSkipHighlighting(false),
        "Expected no default values");

    assertEquals(defaultFlags, copyFlags, "Expected defaults to be unmodified");
  }
}
