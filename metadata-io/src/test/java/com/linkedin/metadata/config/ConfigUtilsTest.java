package com.linkedin.metadata.config;

import static org.testng.Assert.*;

import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ConfigUtilsTest {

  private ResultsLimitConfig resultsLimitConfig;
  private LimitConfig limitConfig;

  @BeforeMethod
  public void setUp() {
    // Default configuration for most tests
    resultsLimitConfig = ResultsLimitConfig.builder().max(100).apiDefault(20).strict(false).build();

    limitConfig = LimitConfig.builder().results(resultsLimitConfig).build();
  }

  @Test
  public void testApplyLimitWithSearchServiceConfiguration_NullLimit() {
    SearchServiceConfiguration config =
        SearchServiceConfiguration.builder().limit(limitConfig).build();

    int result = ConfigUtils.applyLimit(config, null);
    assertEquals(result, 20, "Should return apiDefault when limit is null");
  }

  @Test
  public void testApplyLimitWithSearchServiceConfiguration_ValidLimit() {
    SearchServiceConfiguration config =
        SearchServiceConfiguration.builder().limit(limitConfig).build();

    int result = ConfigUtils.applyLimit(config, 50);
    assertEquals(result, 50, "Should return the provided limit when it's within bounds");
  }

  @Test
  public void testApplyLimitWithGraphServiceConfiguration_NullLimit() {
    GraphServiceConfiguration config =
        GraphServiceConfiguration.builder().limit(limitConfig).build();

    int result = ConfigUtils.applyLimit(config, null);
    assertEquals(result, 20, "Should return apiDefault when limit is null");
  }

  @Test
  public void testApplyLimitWithGraphServiceConfiguration_ValidLimit() {
    GraphServiceConfiguration config =
        GraphServiceConfiguration.builder().limit(limitConfig).build();

    int result = ConfigUtils.applyLimit(config, 75);
    assertEquals(result, 75, "Should return the provided limit when it's within bounds");
  }

  @Test
  public void testApplyLimitWithSystemMetadataServiceConfig_NullLimit() {
    SystemMetadataServiceConfig config =
        SystemMetadataServiceConfig.builder().limit(limitConfig).build();

    int result = ConfigUtils.applyLimit(config, null);
    assertEquals(result, 20, "Should return apiDefault when limit is null");
  }

  @Test
  public void testApplyLimitWithSystemMetadataServiceConfig_ValidLimit() {
    SystemMetadataServiceConfig config =
        SystemMetadataServiceConfig.builder().limit(limitConfig).build();

    int result = ConfigUtils.applyLimit(config, 30);
    assertEquals(result, 30, "Should return the provided limit when it's within bounds");
  }

  @Test
  public void testApplyLimitWithTimeseriesAspectServiceConfig_NullLimit() {
    TimeseriesAspectServiceConfig config =
        TimeseriesAspectServiceConfig.builder().limit(limitConfig).build();

    int result = ConfigUtils.applyLimit(config, null);
    assertEquals(result, 20, "Should return apiDefault when limit is null");
  }

  @Test
  public void testApplyLimitWithTimeseriesAspectServiceConfig_ValidLimit() {
    TimeseriesAspectServiceConfig config =
        TimeseriesAspectServiceConfig.builder().limit(limitConfig).build();

    int result = ConfigUtils.applyLimit(config, 40);
    assertEquals(result, 40, "Should return the provided limit when it's within bounds");
  }

  @Test
  public void testApplyLimitExceedsMax_NonStrict() {
    // Non-strict mode: should return apiDefault and log warning
    SearchServiceConfiguration config =
        SearchServiceConfiguration.builder().limit(limitConfig).build();

    int result = ConfigUtils.applyLimit(config, 150);
    assertEquals(result, 20, "Should return apiDefault when limit exceeds max in non-strict mode");
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = "Result count exceeds limit of 100")
  public void testApplyLimitExceedsMax_Strict() {
    // Strict mode: should throw exception
    ResultsLimitConfig strictConfig =
        ResultsLimitConfig.builder().max(100).apiDefault(20).strict(true).build();

    LimitConfig strictLimitConfig = LimitConfig.builder().results(strictConfig).build();

    SearchServiceConfiguration config =
        SearchServiceConfiguration.builder().limit(strictLimitConfig).build();

    ConfigUtils.applyLimit(config, 150); // Should throw exception
  }

  @DataProvider(name = "limitValues")
  public Object[][] provideLimitValues() {
    return new Object[][] {
      {null, 20}, // null limit returns apiDefault
      {-1, 20}, // negative limit returns apiDefault
      {-100, 20}, // negative limit returns apiDefault
      {0, 0}, // zero is a valid limit
      {10, 10}, // valid limit returns itself
      {50, 50}, // valid limit returns itself
      {100, 100}, // exactly max is valid
      {101, 20}, // exceeds max returns apiDefault (non-strict)
      {200, 20} // exceeds max returns apiDefault (non-strict)
    };
  }

  @Test(dataProvider = "limitValues")
  public void testApplyLimitWithResultsLimitConfig_DataDriven(
      Integer inputLimit, int expectedResult) {
    int result = ConfigUtils.applyLimit(resultsLimitConfig, inputLimit);
    assertEquals(result, expectedResult);
  }

  @Test
  public void testApplyLimitWithZeroLimit() {
    SearchServiceConfiguration config =
        SearchServiceConfiguration.builder().limit(limitConfig).build();

    int result = ConfigUtils.applyLimit(config, 0);
    assertEquals(result, 0, "Should allow zero as a valid limit");
  }

  @Test
  public void testApplyLimitWithNegativeLimit() {
    SearchServiceConfiguration config =
        SearchServiceConfiguration.builder().limit(limitConfig).build();

    int result = ConfigUtils.applyLimit(config, -1);
    assertEquals(result, 20, "Should return apiDefault when limit is negative");
  }

  @Test
  public void testApplyLimitEdgeCase_MaxEqualsApiDefault() {
    ResultsLimitConfig edgeConfig =
        ResultsLimitConfig.builder().max(50).apiDefault(50).strict(false).build();

    int result = ConfigUtils.applyLimit(edgeConfig, 60);
    assertEquals(result, 50, "Should return apiDefault when limit exceeds max");
  }

  @Test
  public void testApplyLimitWithVariousNegativeValues() {
    SearchServiceConfiguration config =
        SearchServiceConfiguration.builder().limit(limitConfig).build();

    // Test various negative values
    assertEquals(ConfigUtils.applyLimit(config, -1), 20, "Should return apiDefault for -1");
    assertEquals(ConfigUtils.applyLimit(config, -10), 20, "Should return apiDefault for -10");
    assertEquals(ConfigUtils.applyLimit(config, -999), 20, "Should return apiDefault for -999");
    assertEquals(
        ConfigUtils.applyLimit(config, Integer.MIN_VALUE),
        20,
        "Should return apiDefault for Integer.MIN_VALUE");
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testApplyLimitWithNullConfig() {
    // This test verifies that @Nonnull annotation is working as expected
    ConfigUtils.applyLimit((SearchServiceConfiguration) null, 50);
  }

  @Test
  public void testMultipleConfigTypes_SameBehavior() {
    // Ensure all config types behave consistently
    SearchServiceConfiguration searchConfig =
        SearchServiceConfiguration.builder().limit(limitConfig).build();
    GraphServiceConfiguration graphConfig =
        GraphServiceConfiguration.builder().limit(limitConfig).build();
    SystemMetadataServiceConfig systemConfig =
        SystemMetadataServiceConfig.builder().limit(limitConfig).build();
    TimeseriesAspectServiceConfig timeseriesConfig =
        TimeseriesAspectServiceConfig.builder().limit(limitConfig).build();

    int limit = 45;

    assertEquals(ConfigUtils.applyLimit(searchConfig, limit), 45);
    assertEquals(ConfigUtils.applyLimit(graphConfig, limit), 45);
    assertEquals(ConfigUtils.applyLimit(systemConfig, limit), 45);
    assertEquals(ConfigUtils.applyLimit(timeseriesConfig, limit), 45);
  }
}
