package com.linkedin.datahub.graphql.types.corpuser.mappers;

import static org.testng.Assert.*;

import com.linkedin.data.template.DoubleMap;
import com.linkedin.datahub.graphql.generated.CorpUserUsageFeatures;
import com.linkedin.datahub.graphql.generated.FloatMapEntry;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class CorpUserUsageFeaturesMapperTest {

  private static final Long TEST_QUERY_COUNT = 1500L;
  private static final String TEST_PLATFORM_SNOWFLAKE = "snowflake";
  private static final String TEST_PLATFORM_REDSHIFT = "redshift";
  private static final Double TEST_SNOWFLAKE_TOTAL = 850.0;
  private static final Double TEST_REDSHIFT_TOTAL = 650.0;
  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)";
  private static final Double TEST_DATASET_USAGE = 125.5;

  @Test
  public void testMapWithAllFields() {
    // Create test input
    final com.linkedin.metadata.search.features.CorpUserUsageFeatures input =
        new com.linkedin.metadata.search.features.CorpUserUsageFeatures();

    input.setUserUsageTotalPast30Days(TEST_QUERY_COUNT);

    // Set platform usage totals
    Map<String, Double> platformTotalsMap = new HashMap<>();
    platformTotalsMap.put(TEST_PLATFORM_SNOWFLAKE, TEST_SNOWFLAKE_TOTAL);
    platformTotalsMap.put(TEST_PLATFORM_REDSHIFT, TEST_REDSHIFT_TOTAL);
    DoubleMap platformTotals = new DoubleMap(platformTotalsMap);
    input.setUserPlatformUsageTotalsPast30Days(platformTotals);

    // Set platform usage percentiles
    Map<String, Double> platformPercentilesMap = new HashMap<>();
    platformPercentilesMap.put(TEST_PLATFORM_SNOWFLAKE, 85.5);
    platformPercentilesMap.put(TEST_PLATFORM_REDSHIFT, 92.1);
    DoubleMap platformPercentiles = new DoubleMap(platformPercentilesMap);
    input.setUserPlatformUsagePercentilePast30Days(platformPercentiles);

    // Set top datasets
    Map<String, Double> topDatasetsMap = new HashMap<>();
    topDatasetsMap.put(TEST_DATASET_URN, TEST_DATASET_USAGE);
    DoubleMap topDatasets = new DoubleMap(topDatasetsMap);
    input.setUserTopDatasetsByUsage(topDatasets);

    // Map to GraphQL type
    CorpUserUsageFeatures result = CorpUserUsageFeaturesMapper.map(null, input);

    // Verify all fields are mapped correctly
    assertNotNull(result);
    assertEquals(result.getUserUsageTotalPast30Days(), TEST_QUERY_COUNT);

    // Verify platform totals
    assertNotNull(result.getUserPlatformUsageTotalsPast30Days());
    assertEquals(result.getUserPlatformUsageTotalsPast30Days().size(), 2);

    FloatMapEntry snowflakeTotal =
        result.getUserPlatformUsageTotalsPast30Days().stream()
            .filter(entry -> TEST_PLATFORM_SNOWFLAKE.equals(entry.getKey()))
            .findFirst()
            .orElse(null);
    assertNotNull(snowflakeTotal);
    assertEquals(snowflakeTotal.getValue(), TEST_SNOWFLAKE_TOTAL.floatValue(), 0.01);

    FloatMapEntry redshiftTotal =
        result.getUserPlatformUsageTotalsPast30Days().stream()
            .filter(entry -> TEST_PLATFORM_REDSHIFT.equals(entry.getKey()))
            .findFirst()
            .orElse(null);
    assertNotNull(redshiftTotal);
    assertEquals(redshiftTotal.getValue(), TEST_REDSHIFT_TOTAL.floatValue(), 0.01);

    // Verify platform percentiles
    assertNotNull(result.getUserPlatformUsagePercentilePast30Days());
    assertEquals(result.getUserPlatformUsagePercentilePast30Days().size(), 2);

    FloatMapEntry snowflakePercentile =
        result.getUserPlatformUsagePercentilePast30Days().stream()
            .filter(entry -> TEST_PLATFORM_SNOWFLAKE.equals(entry.getKey()))
            .findFirst()
            .orElse(null);
    assertNotNull(snowflakePercentile);
    assertEquals(snowflakePercentile.getValue(), 85.5f, 0.01);

    // Verify top datasets
    assertNotNull(result.getUserTopDatasetsByUsage());
    assertEquals(result.getUserTopDatasetsByUsage().size(), 1);

    FloatMapEntry datasetUsage = result.getUserTopDatasetsByUsage().get(0);
    assertEquals(datasetUsage.getKey(), TEST_DATASET_URN);
    assertEquals(datasetUsage.getValue(), TEST_DATASET_USAGE.floatValue(), 0.01);
  }

  @Test
  public void testMapWithOnlyQueryCount() {
    final com.linkedin.metadata.search.features.CorpUserUsageFeatures input =
        new com.linkedin.metadata.search.features.CorpUserUsageFeatures();

    input.setUserUsageTotalPast30Days(TEST_QUERY_COUNT);

    CorpUserUsageFeatures result = CorpUserUsageFeaturesMapper.map(null, input);

    assertNotNull(result);
    assertEquals(result.getUserUsageTotalPast30Days(), TEST_QUERY_COUNT);
    assertNull(result.getUserPlatformUsageTotalsPast30Days());
    assertNull(result.getUserPlatformUsagePercentilePast30Days());
    assertNull(result.getUserTopDatasetsByUsage());
  }

  @Test
  public void testMapWithEmptyMaps() {
    final com.linkedin.metadata.search.features.CorpUserUsageFeatures input =
        new com.linkedin.metadata.search.features.CorpUserUsageFeatures();

    input.setUserUsageTotalPast30Days(TEST_QUERY_COUNT);
    input.setUserPlatformUsageTotalsPast30Days(new DoubleMap());
    input.setUserPlatformUsagePercentilePast30Days(new DoubleMap());
    input.setUserTopDatasetsByUsage(new DoubleMap());

    CorpUserUsageFeatures result = CorpUserUsageFeaturesMapper.map(null, input);

    assertNotNull(result);
    assertEquals(result.getUserUsageTotalPast30Days(), TEST_QUERY_COUNT);

    // Empty maps should result in empty lists
    assertNotNull(result.getUserPlatformUsageTotalsPast30Days());
    assertTrue(result.getUserPlatformUsageTotalsPast30Days().isEmpty());

    assertNotNull(result.getUserPlatformUsagePercentilePast30Days());
    assertTrue(result.getUserPlatformUsagePercentilePast30Days().isEmpty());

    assertNotNull(result.getUserTopDatasetsByUsage());
    assertTrue(result.getUserTopDatasetsByUsage().isEmpty());
  }

  @Test
  public void testMapWithNoFields() {
    final com.linkedin.metadata.search.features.CorpUserUsageFeatures input =
        new com.linkedin.metadata.search.features.CorpUserUsageFeatures();

    CorpUserUsageFeatures result = CorpUserUsageFeaturesMapper.map(null, input);

    assertNotNull(result);
    assertNull(result.getUserUsageTotalPast30Days());
    assertNull(result.getUserPlatformUsageTotalsPast30Days());
    assertNull(result.getUserPlatformUsagePercentilePast30Days());
    assertNull(result.getUserTopDatasetsByUsage());
  }

  @Test
  public void testDoubleToFloatConversion() {
    final com.linkedin.metadata.search.features.CorpUserUsageFeatures input =
        new com.linkedin.metadata.search.features.CorpUserUsageFeatures();

    // Test with precise double values that could lose precision as float
    Map<String, Double> platformTotalsMap = new HashMap<>();
    platformTotalsMap.put("test", 123.456789);
    DoubleMap platformTotals = new DoubleMap(platformTotalsMap);
    input.setUserPlatformUsageTotalsPast30Days(platformTotals);

    CorpUserUsageFeatures result = CorpUserUsageFeaturesMapper.map(null, input);

    assertNotNull(result);
    assertNotNull(result.getUserPlatformUsageTotalsPast30Days());
    assertEquals(result.getUserPlatformUsageTotalsPast30Days().size(), 1);

    FloatMapEntry entry = result.getUserPlatformUsageTotalsPast30Days().get(0);
    assertEquals(entry.getKey(), "test");
    // Float precision should be maintained within reasonable bounds
    assertEquals(entry.getValue(), 123.456789f, 0.0001);
  }
}
