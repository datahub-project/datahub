package datahub.spark;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.spark.conf.SparkAppContext;
import datahub.spark.conf.SparkLineageConf;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class StreamingEventCorrelatorTest {

  @Mock private StreamingQueryProgress mockStreamingQueryProgress;
  @Mock private SparkAppContext mockSparkAppContext;
  @Mock private SparkConf mockSparkConf;

  private StreamingEventCorrelator correlator;
  private DatahubOpenlineageConfig openLineageConfig;
  private SparkLineageConf sparkLineageConf;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    // Set up mock behaviors
    when(mockStreamingQueryProgress.id())
        .thenReturn(
            new org.apache.spark.sql.streaming.StreamingQueryId() {
              @Override
              public String toString() {
                return "test-query-id";
              }
            });
    when(mockStreamingQueryProgress.json()).thenReturn("{}");
    when(mockStreamingQueryProgress.batchId()).thenReturn(1L);
    when(mockStreamingQueryProgress.name()).thenReturn("test-query");

    when(mockSparkAppContext.getConf()).thenReturn(mockSparkConf);

    // Create basic OpenLineage config
    openLineageConfig =
        DatahubOpenlineageConfig.builder()
            .isSpark(true)
            .fabricType(FabricType.PROD)
            .usePatch(false) // Default to not using patch
            .materializeDataset(true)
            .build();

    // Create SparkLineage config
    sparkLineageConf =
        SparkLineageConf.builder()
            .sparkAppContext(mockSparkAppContext)
            .openLineageConf(openLineageConfig)
            .build();

    // Create correlator
    correlator = new StreamingEventCorrelator(sparkLineageConf);
  }

  @Test
  public void testPatchingEnabledFromSparkConf() {
    // Set spark.datahub.patch.enabled=true in SparkConf
    when(mockSparkConf.get("spark.datahub.patch.enabled", "false")).thenReturn("true");

    // Simulate receiving catalog table information
    String queryId = "test-query-id";

    // Create test datasets
    Set<DatasetUrn> inputDatasets = new HashSet<>();
    inputDatasets.add(new DatasetUrn(new DataPlatformUrn("kafka"), "test-topic", FabricType.PROD));

    Set<DatasetUrn> outputDatasets = new HashSet<>();
    outputDatasets.add(new DatasetUrn(new DataPlatformUrn("hive"), "test.table", FabricType.PROD));

    // Add the streaming event data using reflection (since we don't have public API access)
    try {
      // Create StreamingEventData through reflection
      java.lang.reflect.Method recordCatalogTableMethod =
          StreamingEventCorrelator.class.getDeclaredMethod(
              "recordCatalogTable", String.class, java.util.Map.class);
      recordCatalogTableMethod.setAccessible(true);

      // Create metadata map
      java.util.Map<String, String> metadata = new java.util.HashMap<>();
      metadata.put("catalogName", "test");
      metadata.put("databaseName", "test");
      metadata.put("tableName", "table");

      // Call the method
      recordCatalogTableMethod.invoke(correlator, queryId, metadata);

      // Set the event data with mock StreamingQueryProgress
      java.lang.reflect.Field eventDataByQueryIdField =
          StreamingEventCorrelator.class.getDeclaredField("eventDataByQueryId");
      eventDataByQueryIdField.setAccessible(true);

      java.util.Map<String, Object> eventDataByQueryId =
          (java.util.Map<String, Object>) eventDataByQueryIdField.get(correlator);

      Object eventData = eventDataByQueryId.get(queryId);

      // Set input/output datasets
      java.lang.reflect.Field inputDatasetsField =
          eventData.getClass().getDeclaredField("inputDatasets");
      inputDatasetsField.setAccessible(true);
      Set<DatasetUrn> currentInputs = (Set<DatasetUrn>) inputDatasetsField.get(eventData);
      currentInputs.addAll(inputDatasets);

      java.lang.reflect.Field outputDatasetsField =
          eventData.getClass().getDeclaredField("outputDatasets");
      outputDatasetsField.setAccessible(true);
      Set<DatasetUrn> currentOutputs = (Set<DatasetUrn>) outputDatasetsField.get(eventData);
      currentOutputs.addAll(outputDatasets);

      java.lang.reflect.Field eventField = eventData.getClass().getDeclaredField("event");
      eventField.setAccessible(true);
      eventField.set(eventData, mockStreamingQueryProgress);

      // Generate lineage from the streaming data
      java.lang.reflect.Method generateLineageMethod =
          StreamingEventCorrelator.class.getDeclaredMethod(
              "generateLineageFromStreamingData", String.class);
      generateLineageMethod.setAccessible(true);

      // Call the method and get MCPs
      List<MetadataChangeProposalWrapper> mcps =
          (List<MetadataChangeProposalWrapper>) generateLineageMethod.invoke(correlator, queryId);

      // Verify the results
      assertNotNull("MCPs should not be null", mcps);
      assertFalse("MCPs should not be empty", mcps.isEmpty());

      // We can't directly verify the patch format in the test without more mocking,
      // but we can verify the pipeline was invoked with proper parameters
      assertTrue("Patching should be enabled", isPatchingEnabledInConfig(correlator));

    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception during test: " + e.getMessage());
    }
  }

  @Test
  public void testPatchingEnabledFromOpenLineageConfig() {
    // Set usePatch=true in OpenLineageConfig
    openLineageConfig =
        DatahubOpenlineageConfig.builder()
            .isSpark(true)
            .fabricType(FabricType.PROD)
            .usePatch(true) // Enable patching
            .materializeDataset(true)
            .build();

    // Update SparkLineage config
    sparkLineageConf =
        SparkLineageConf.builder()
            .sparkAppContext(mockSparkAppContext)
            .openLineageConf(openLineageConfig)
            .build();

    // Update correlator
    correlator = new StreamingEventCorrelator(sparkLineageConf);

    // Verify patching is enabled
    assertTrue("Patching should be enabled", isPatchingEnabledInConfig(correlator));
  }

  // Helper method to check if patching is enabled using reflection
  private boolean isPatchingEnabledInConfig(StreamingEventCorrelator correlator) {
    try {
      java.lang.reflect.Method isPatchingEnabledMethod =
          StreamingEventCorrelator.class.getDeclaredMethod("isPatchingEnabled");
      isPatchingEnabledMethod.setAccessible(true);

      return (boolean) isPatchingEnabledMethod.invoke(correlator);
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }
}
