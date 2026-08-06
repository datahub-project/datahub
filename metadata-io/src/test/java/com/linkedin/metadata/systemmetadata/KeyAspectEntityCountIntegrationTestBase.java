package com.linkedin.metadata.systemmetadata;

import static io.datahubproject.test.search.SearchTestUtils.TEST_SYSTEM_METADATA_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.cache.KeyAspectEntityCountCacheConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.systemmetadata.cache.KeyAspectEntityCountCache;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.SearchTestUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration tests for key-aspect entity counts against a live Elasticsearch or OpenSearch
 * Testcontainers instance.
 */
public abstract class KeyAspectEntityCountIntegrationTestBase
    extends AbstractTestNGSpringContextTests {

  @Nonnull
  protected abstract SearchClientShim<?> getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  @Nonnull
  protected abstract ESIndexBuilder getIndexBuilder();

  protected OperationContext operationContext;
  private ElasticSearchSystemMetadataService systemMetadataService;
  private KeyAspectEntityCountService keyAspectEntityCountService;

  private final IndexConvention indexConvention =
      new IndexConventionImpl(
          IndexConventionImpl.IndexConventionConfig.builder()
              .prefix("es_key_aspect_entity_count_test")
              .hashIdAlgo("MD5")
              .build(),
          SearchTestUtils.DEFAULT_ENTITY_INDEX_CONFIGURATION);

  @BeforeClass
  public void setup() {
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    systemMetadataService = buildSystemMetadataService();
    keyAspectEntityCountService = buildKeyAspectEntityCountService();
    systemMetadataService.reindexAll(operationContext, Collections.emptySet());
  }

  @BeforeMethod
  public void wipe() throws Exception {
    syncAfterWrite(getBulkProcessor());
    systemMetadataService.clear(operationContext);
    syncAfterWrite(getBulkProcessor());
  }

  @Test
  public void testCountByKeyAspectAgainstSearchBackend() throws Exception {
    seedActiveAndSoftDeletedChartEntities();

    KeyAspectCount chartCount =
        systemMetadataService.countByKeyAspect(operationContext, "chartKey");
    assertEquals(chartCount.getActiveCount(), 1L);
    assertEquals(chartCount.getSoftDeletedCount(), 1L);

    Map<String, KeyAspectCount> batchCounts =
        systemMetadataService.countByKeyAspects(
            operationContext, List.of("chartKey", "datasetKey"));
    assertEquals(batchCounts.get("chartKey").getActiveCount(), 1L);
    assertEquals(batchCounts.get("chartKey").getSoftDeletedCount(), 1L);
    assertEquals(batchCounts.get("datasetKey").getActiveCount(), 1L);
    assertEquals(batchCounts.get("datasetKey").getSoftDeletedCount(), 0L);
  }

  @Test
  public void testKeyAspectEntityCountServiceSingleType() throws Exception {
    seedActiveAndSoftDeletedChartEntities();

    KeyAspectEntityCountResult result =
        keyAspectEntityCountService.getCountForEntityType(operationContext, "chart", true);

    assertEquals(result.getCounts().size(), 1);
    assertEquals(result.getCounts().get(0).getEntityType(), "chart");
    assertEquals(result.getCounts().get(0).getKeyAspect(), "chartKey");
    assertEquals(result.getCounts().get(0).getActiveCount(), 1L);
    assertEquals(result.getCounts().get(0).getSoftDeletedCount(), 1L);
    assertEquals(result.getCounts().get(0).totalCount(), 2L);
  }

  @Test
  public void testKeyAspectEntityCountServiceBatchTypes() throws Exception {
    seedActiveAndSoftDeletedChartEntities();

    KeyAspectEntityCountResult result =
        keyAspectEntityCountService.getCounts(operationContext, List.of("dataset", "chart"), true);

    assertEquals(result.getRequestedTypes(), List.of("chart", "dataset"));
    assertEquals(result.getCounts().size(), 2);
    assertEquals(result.activeTotal(), 2L);
    assertEquals(result.softDeletedTotal(), 1L);
    assertEquals(result.totalCount(), 3L);
  }

  @Test
  public void testEmptyIndexReturnsZeroCounts() {
    KeyAspectCount chartCount =
        systemMetadataService.countByKeyAspect(operationContext, "chartKey");
    assertEquals(chartCount.getActiveCount(), 0L);
    assertEquals(chartCount.getSoftDeletedCount(), 0L);

    KeyAspectEntityCountResult result =
        keyAspectEntityCountService.getCountForEntityType(operationContext, "chart", true);
    assertEquals(result.getCounts().get(0).getActiveCount(), 0L);
    assertEquals(result.getCounts().get(0).getSoftDeletedCount(), 0L);
  }

  private void seedActiveAndSoftDeletedChartEntities() throws Exception {
    SystemMetadata metadata = new SystemMetadata();
    metadata.setRunId("count-run");
    metadata.setLastObserved(100L);

    systemMetadataService.insert(operationContext, metadata, "urn:li:chart:active", "chartKey");
    systemMetadataService.insert(operationContext, metadata, "urn:li:chart:deleted", "chartKey");
    systemMetadataService.insert(operationContext, metadata, "urn:li:dataset:active", "datasetKey");
    syncAfterWrite(getBulkProcessor());

    systemMetadataService.setDocStatus(operationContext, "urn:li:chart:deleted", true);
    syncAfterWrite(getBulkProcessor());
  }

  @Nonnull
  private ElasticSearchSystemMetadataService buildSystemMetadataService() {
    ESSystemMetadataDAO dao =
        new ESSystemMetadataDAO(
            getSearchClient(),
            indexConvention,
            getBulkProcessor(),
            1,
            TEST_SYSTEM_METADATA_SERVICE_CONFIG);
    return new ElasticSearchSystemMetadataService(
        getBulkProcessor(),
        indexConvention,
        dao,
        getIndexBuilder(),
        "MD5",
        TEST_SYSTEM_METADATA_SERVICE_CONFIG);
  }

  @Nonnull
  private KeyAspectEntityCountService buildKeyAspectEntityCountService() {
    EntityRegistry entityRegistry = TestOperationContexts.defaultEntityRegistry();
    KeyAspectEntityCountCacheConfiguration cacheConfig =
        new KeyAspectEntityCountCacheConfiguration();
    cacheConfig.setEnabled(false);
    KeyAspectEntityCountCache cache = new KeyAspectEntityCountCache(cacheConfig, null);
    return new KeyAspectEntityCountService(entityRegistry, systemMetadataService, cache, 200);
  }
}
