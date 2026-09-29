package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es8SearchClientShim.IndexSettingsComparison;
import java.util.Set;
import org.opensearch.common.settings.Settings;
import org.testng.annotations.Test;

public class Es8SearchClientShimIndexSettingsComparisonTest {

  @Test
  public void testStoredNamesForComparisonIgnoresInjectedCustomTypeWhenTargetOmitsType() {
    Settings stored =
        Settings.builder().put("type", "custom").put("tokenizer", "path_hierarchy").build();

    Set<String> names =
        IndexSettingsComparison.storedNamesForComparison(
            ImmutableMap.of("tokenizer", "path_hierarchy"), stored);

    assertFalse(names.contains("type"));
    assertTrue(names.contains("tokenizer"));
  }

  @Test
  public void testStoredNamesForComparisonKeepsNonCustomTypeWhenTargetOmitsType() {
    Settings stored =
        Settings.builder().put("type", "standard").put("tokenizer", "standard").build();

    Set<String> names =
        IndexSettingsComparison.storedNamesForComparison(
            ImmutableMap.of("tokenizer", "standard"), stored);

    assertTrue(names.contains("type"));
  }

  @Test
  public void testStoredNamesForComparisonKeepsTypeWhenTargetIncludesType() {
    Settings stored =
        Settings.builder().put("type", "custom").put("tokenizer", "path_hierarchy").build();

    Set<String> names =
        IndexSettingsComparison.storedNamesForComparison(
            ImmutableMap.of("type", "custom", "tokenizer", "path_hierarchy"), stored);

    assertTrue(names.contains("type"));
  }

  @Test
  public void testValuesEqualIsCaseInsensitiveAfterStrictMatchFails() {
    assertTrue(IndexSettingsComparison.valuesEqual("True", "true"));
    assertTrue(IndexSettingsComparison.valuesEqual("CUSTOM", "custom"));
    assertFalse(IndexSettingsComparison.valuesEqual("keyword", "standard"));
  }

  @Test
  public void testValuesEqualStrictMatchPreferred() {
    assertTrue(IndexSettingsComparison.valuesEqual("exact", "exact"));
    assertFalse(IndexSettingsComparison.valuesEqual(null, "x"));
  }
}
