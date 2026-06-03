package com.linkedin.metadata.utils.elasticsearch;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Set;
import org.opensearch.common.settings.Settings;
import org.testng.annotations.Test;

public class IndexSettingsComparisonTest {

  @Test
  public void testStrictIndexSettingNamesForComparisonUsesStoredNames() {
    Settings stored =
        Settings.builder().put("tokenizer", "standard").put("filter", "lowercase").build();
    Set<String> names =
        IndexSettingsComparison.Strict.INSTANCE.indexSettingNamesForComparison(
            ImmutableMap.of("tokenizer", "standard"), stored);

    assertTrue(names.contains("tokenizer"));
    assertTrue(names.contains("filter"));
  }

  @Test
  public void testStrictIndexSettingValuesEqualMatchesStrings() {
    assertTrue(IndexSettingsComparison.Strict.INSTANCE.indexSettingValuesEqual("true", "true"));
    assertFalse(IndexSettingsComparison.Strict.INSTANCE.indexSettingValuesEqual("true", "false"));
  }

  @Test
  public void testStrictIndexSettingValuesEqualHandlesNulls() {
    assertTrue(IndexSettingsComparison.Strict.INSTANCE.indexSettingValuesEqual(null, null));
    assertFalse(IndexSettingsComparison.Strict.INSTANCE.indexSettingValuesEqual("x", null));
    assertFalse(IndexSettingsComparison.Strict.INSTANCE.indexSettingValuesEqual(null, "x"));
  }

  @Test
  public void testStrictIndexSettingValuesEqualUsesToString() {
    assertTrue(IndexSettingsComparison.Strict.INSTANCE.indexSettingValuesEqual(2, "2"));
    assertFalse(IndexSettingsComparison.Strict.INSTANCE.indexSettingValuesEqual(2, "3"));
  }
}
