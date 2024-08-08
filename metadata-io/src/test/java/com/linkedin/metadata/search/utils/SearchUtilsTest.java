package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.assertEquals;

import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.query.GroupingCriterion;
import com.linkedin.metadata.query.GroupingCriterionArray;
import com.linkedin.metadata.query.GroupingSpec;
import com.linkedin.metadata.query.SearchFlags;
import java.util.Set;
import org.testng.annotations.Test;

public class SearchUtilsTest {

  private SearchFlags getDefaultSearchFlags() {
    return setConvertSchemaFieldsToDatasets(
        new SearchFlags()
            .setFulltext(true)
            .setSkipCache(true)
            .setSkipAggregates(true)
            .setMaxAggValues(1)
            .setSkipHighlighting(true)
            .setIncludeSoftDeleted(false)
            .setIncludeRestricted(false),
        true);
  }

  private SearchFlags setConvertSchemaFieldsToDatasets(SearchFlags flags, boolean value) {
    if (value) {
      return flags.setGroupingSpec(
          new GroupingSpec()
              .setGroupingCriteria(
                  new GroupingCriterionArray(
                      new GroupingCriterion()
                          .setBaseEntityType(SCHEMA_FIELD_ENTITY_NAME)
                          .setGroupingEntityType(DATASET_ENTITY_NAME))));
    } else {
      return flags.setGroupingSpec(null, SetMode.REMOVE_IF_NULL);
    }
  }

  @Test
  public void testApplyDefaultSearchFlags() {
    SearchFlags defaultFlags = getDefaultSearchFlags();

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
                .setSkipHighlighting(false)
                .setIncludeSoftDeleted(false)
                .setIncludeRestricted(false),
            "not empty",
            defaultFlags),
        setConvertSchemaFieldsToDatasets(
            new SearchFlags()
                .setFulltext(false)
                .setSkipAggregates(false)
                .setSkipCache(false)
                .setMaxAggValues(2)
                .setSkipHighlighting(false)
                .setIncludeSoftDeleted(false)
                .setIncludeRestricted(false),
            SearchUtils.convertSchemaFieldToDataset(defaultFlags)),
        "Expected no default values");

    assertEquals(
        SearchUtils.applyDefaultSearchFlags(
            new SearchFlags()
                .setFulltext(false)
                .setSkipCache(false)
                .setSkipAggregates(false)
                .setMaxAggValues(2)
                .setSkipHighlighting(false)
                .setIncludeSoftDeleted(false)
                .setIncludeRestricted(false),
            null,
            defaultFlags),
        setConvertSchemaFieldsToDatasets(
            new SearchFlags()
                .setFulltext(false)
                .setSkipAggregates(false)
                .setSkipCache(false)
                .setMaxAggValues(2)
                .setSkipHighlighting(true)
                .setIncludeSoftDeleted(false)
                .setIncludeRestricted(false),
            SearchUtils.convertSchemaFieldToDataset(defaultFlags)),
        "Expected skip highlight due to query null query");
    for (String query : Set.of("*", "")) {
      assertEquals(
          SearchUtils.applyDefaultSearchFlags(
              new SearchFlags()
                  .setFulltext(false)
                  .setSkipCache(false)
                  .setSkipAggregates(false)
                  .setMaxAggValues(2)
                  .setSkipHighlighting(false)
                  .setIncludeSoftDeleted(false)
                  .setIncludeRestricted(false),
              query,
              defaultFlags),
          setConvertSchemaFieldsToDatasets(
              new SearchFlags()
                  .setFulltext(false)
                  .setSkipAggregates(false)
                  .setSkipCache(false)
                  .setMaxAggValues(2)
                  .setSkipHighlighting(true)
                  .setIncludeSoftDeleted(false)
                  .setIncludeRestricted(false),
              SearchUtils.convertSchemaFieldToDataset(defaultFlags)),
          String.format("Expected skip highlight due to query string `%s`", query));
    }

    assertEquals(
        SearchUtils.applyDefaultSearchFlags(
            new SearchFlags().setFulltext(false), "not empty", defaultFlags),
        setConvertSchemaFieldsToDatasets(
            new SearchFlags()
                .setFulltext(false)
                .setSkipAggregates(true)
                .setSkipCache(true)
                .setMaxAggValues(1)
                .setSkipHighlighting(true)
                .setIncludeSoftDeleted(false)
                .setIncludeRestricted(false),
            SearchUtils.convertSchemaFieldToDataset(defaultFlags)),
        "Expected all default values except fulltext");
    assertEquals(
        SearchUtils.applyDefaultSearchFlags(
            new SearchFlags().setSkipCache(false), "not empty", defaultFlags),
        setConvertSchemaFieldsToDatasets(
            new SearchFlags()
                .setFulltext(true)
                .setSkipAggregates(true)
                .setSkipCache(false)
                .setMaxAggValues(1)
                .setSkipHighlighting(true)
                .setIncludeSoftDeleted(false)
                .setIncludeRestricted(false),
            SearchUtils.convertSchemaFieldToDataset(defaultFlags)),
        "Expected all default values except skipCache");
    assertEquals(
        SearchUtils.applyDefaultSearchFlags(
            new SearchFlags().setSkipAggregates(false), "not empty", defaultFlags),
        setConvertSchemaFieldsToDatasets(
            new SearchFlags()
                .setFulltext(true)
                .setSkipAggregates(false)
                .setSkipCache(true)
                .setMaxAggValues(1)
                .setSkipHighlighting(true)
                .setIncludeSoftDeleted(false)
                .setIncludeRestricted(false),
            SearchUtils.convertSchemaFieldToDataset(defaultFlags)),
        "Expected all default values except skipAggregates");
    assertEquals(
        SearchUtils.applyDefaultSearchFlags(
            new SearchFlags().setMaxAggValues(2), "not empty", defaultFlags),
        setConvertSchemaFieldsToDatasets(
            new SearchFlags()
                .setFulltext(true)
                .setSkipAggregates(true)
                .setSkipCache(true)
                .setMaxAggValues(2)
                .setSkipHighlighting(true)
                .setIncludeSoftDeleted(false)
                .setIncludeRestricted(false),
            SearchUtils.convertSchemaFieldToDataset(defaultFlags)),
        "Expected all default values except maxAggValues");
    assertEquals(
        SearchUtils.applyDefaultSearchFlags(
            new SearchFlags().setSkipHighlighting(false), "not empty", defaultFlags),
        setConvertSchemaFieldsToDatasets(
            new SearchFlags()
                .setFulltext(true)
                .setSkipAggregates(true)
                .setSkipCache(true)
                .setMaxAggValues(1)
                .setSkipHighlighting(false)
                .setIncludeSoftDeleted(false)
                .setIncludeRestricted(false),
            SearchUtils.convertSchemaFieldToDataset(defaultFlags)),
        "Expected all default values except skipHighlighting");
  }

  @Test
  public void testImmutableDefaults() throws CloneNotSupportedException {
    SearchFlags defaultFlags = getDefaultSearchFlags();

    SearchFlags copyFlags = defaultFlags.copy();

    assertEquals(
        SearchUtils.applyDefaultSearchFlags(
            setConvertSchemaFieldsToDatasets(
                new SearchFlags()
                    .setFulltext(false)
                    .setSkipCache(false)
                    .setSkipAggregates(false)
                    .setMaxAggValues(2)
                    .setSkipHighlighting(false)
                    .setIncludeSoftDeleted(false)
                    .setIncludeRestricted(false),
                SearchUtils.convertSchemaFieldToDataset(defaultFlags)),
            "not empty",
            defaultFlags),
        setConvertSchemaFieldsToDatasets(
            new SearchFlags()
                .setFulltext(false)
                .setSkipAggregates(false)
                .setSkipCache(false)
                .setMaxAggValues(2)
                .setSkipHighlighting(false)
                .setIncludeSoftDeleted(false)
                .setIncludeRestricted(false),
            SearchUtils.convertSchemaFieldToDataset(defaultFlags)),
        "Expected no default values");

    assertEquals(defaultFlags, copyFlags, "Expected defaults to be unmodified");
  }
}
