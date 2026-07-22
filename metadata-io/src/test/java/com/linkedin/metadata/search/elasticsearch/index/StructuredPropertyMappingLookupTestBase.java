package com.linkedin.metadata.search.elasticsearch.index;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.test.search.SearchTestUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.common.settings.Settings;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration tests for {@link SearchEngineStructuredPropertyMappingLookup} against a live
 * Elasticsearch or OpenSearch Testcontainers instance.
 */
public abstract class StructuredPropertyMappingLookupTestBase
    extends AbstractTestNGSpringContextTests {

  private static final String INDEX_PREFIX = "sp_mapping_lookup_it";

  @Nonnull
  protected abstract SearchClientShim<?> getSearchClient();

  private IndexConvention indexConvention;
  private SearchEngineStructuredPropertyMappingLookup lookup;
  private String v2IndexName;
  private String v3IndexName;
  private String unrelatedIndexName;

  @BeforeMethod
  public void setUp() throws IOException {
    indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder()
                .prefix(INDEX_PREFIX)
                .hashIdAlgo("MD5")
                .build(),
            SearchTestUtils.V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION);
    lookup = new SearchEngineStructuredPropertyMappingLookup(getSearchClient(), indexConvention);

    v2IndexName = indexConvention.getEntityIndexName("dataset");
    v3IndexName = indexConvention.getEntityIndexNameV3("dataset");
    unrelatedIndexName = "unrelated-" + UUID.randomUUID() + "-index";

    createIndex(v2IndexName, structuredPropertyFieldMapping("certification_status"));
    createIndex(v3IndexName, versionedStructuredPropertyMapping());
    createIndex(unrelatedIndexName, structuredPropertyFieldMapping("should_not_match"));
  }

  @AfterMethod
  public void tearDown() {
    deleteIndexQuietly(v2IndexName);
    deleteIndexQuietly(v3IndexName);
    deleteIndexQuietly(unrelatedIndexName);
  }

  @Test
  public void testFindsFlatStructuredPropertyFieldFromV2Mapping() throws IOException {
    assertTrue(lookup.fieldExists(OperationFingerprint.EMPTY, "certification_status"));
    assertFalse(lookup.fieldExists(OperationFingerprint.EMPTY, "other_status"));
  }

  @Test
  public void testFindsNestedVersionedFieldFromV3Mapping() throws IOException {
    assertTrue(
        lookup.fieldExists(
            OperationFingerprint.EMPTY, "_versioned.certification_status.00000000000001.string"));
    assertFalse(
        lookup.fieldExists(
            OperationFingerprint.EMPTY, "_versioned.certification_status.00000000000001.number"));
  }

  @Test
  public void testIgnoresMappingsOutsideEntityIndexPatterns() throws IOException {
    assertFalse(lookup.fieldExists(OperationFingerprint.EMPTY, "should_not_match"));
  }

  @Test
  public void testSeesNewlyPutStructuredPropertyMapping() throws IOException {
    assertFalse(lookup.fieldExists(OperationFingerprint.EMPTY, "newly_added_status"));

    PutMappingRequest putMappingRequest = new PutMappingRequest(v2IndexName);
    putMappingRequest.source(structuredPropertyFieldMapping("newly_added_status"));
    getSearchClient()
        .putIndexMapping(OperationFingerprint.EMPTY, putMappingRequest, RequestOptions.DEFAULT);

    assertTrue(lookup.fieldExists(OperationFingerprint.EMPTY, "newly_added_status"));
  }

  @Nonnull
  private static Map<String, Object> structuredPropertyFieldMapping(@Nonnull String fieldName) {
    return properties("structuredProperties", properties(fieldName, Map.of("type", "keyword")));
  }

  @Nonnull
  private static Map<String, Object> versionedStructuredPropertyMapping() {
    return properties(
        "structuredProperties",
        properties(
            "_versioned",
            properties(
                "certification_status",
                properties("00000000000001", properties("string", Map.of("type", "keyword"))))));
  }

  @Nonnull
  private static Map<String, Object> properties(
      @Nonnull String fieldName, @Nonnull Map<String, Object> fieldMapping) {
    Map<String, Object> fields = new HashMap<>();
    fields.put(fieldName, fieldMapping);
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("properties", fields);
    return mapping;
  }

  private void createIndex(@Nonnull String indexName, @Nonnull Map<String, Object> mapping)
      throws IOException {
    CreateIndexRequest createRequest = new CreateIndexRequest(indexName);
    createRequest.settings(
        Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0));
    createRequest.mapping(mapping);
    getSearchClient()
        .createIndex(OperationFingerprint.EMPTY, createRequest, RequestOptions.DEFAULT);
  }

  private void deleteIndexQuietly(@Nonnull String indexName) {
    try {
      getSearchClient()
          .deleteIndex(
              OperationFingerprint.EMPTY,
              new DeleteIndexRequest(indexName),
              RequestOptions.DEFAULT);
    } catch (Exception ignored) {
      // best-effort cleanup between methods / suite teardown
    }
  }
}
