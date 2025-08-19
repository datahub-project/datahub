package com.linkedin.metadata.search.elasticsearch;

import static com.linkedin.metadata.Constants.ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.query.SliceOptions;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchServiceTestBase;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.SearchUtil;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import java.util.Collections;
import org.jetbrains.annotations.NotNull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Import({
  ElasticSearchSuite.class,
  SearchCommonTestConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class SearchServiceElasticSearchTest extends SearchServiceTestBase {

  @Autowired private RestHighLevelClient _searchClient;
  @Autowired private ESBulkProcessor _bulkProcessor;
  @Autowired private ESIndexBuilder _esIndexBuilder;
  @Autowired private SearchConfiguration _searchConfiguration;

  @Autowired
  @Qualifier("defaultTestCustomSearchConfig")
  private CustomSearchConfiguration _customSearchConfiguration;

  @NotNull
  @Override
  protected RestHighLevelClient getSearchClient() {
    return _searchClient;
  }

  @NotNull
  @Override
  protected ESBulkProcessor getBulkProcessor() {
    return _bulkProcessor;
  }

  @NotNull
  @Override
  protected ESIndexBuilder getIndexBuilder() {
    return _esIndexBuilder;
  }

  @NotNull
  @Override
  protected String getElasticSearchImplementation() {
    return ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH;
  }

  @NotNull
  @Override
  protected SearchConfiguration getSearchConfiguration() {
    return _searchConfiguration;
  }

  @Test
  public void initTest() {
    assertNotNull(_searchClient);
  }

  @Test
  public void testSearchWithSliceOptions() throws Exception {
    // Not currently supported for Elasticsearch
    // Testing that this fails gracefully

    // Set up test data
    Urn urn1 = new TestEntityUrn("slice", "testUrn1", "VALUE_1");
    ObjectNode document1 = JsonNodeFactory.instance.objectNode();
    document1.set("urn", JsonNodeFactory.instance.textNode(urn1.toString()));
    document1.set("keyPart1", JsonNodeFactory.instance.textNode("slice_test_data"));
    document1.set("textFieldOverride", JsonNodeFactory.instance.textNode("slice_test_field1"));
    document1.set("browsePaths", JsonNodeFactory.instance.textNode("/slice/test/1"));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document1.toString(), urn1.toString());

    Urn urn2 = new TestEntityUrn("slice", "testUrn2", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("slice_test_data"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("slice_test_field2"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/slice/test/2"));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document2.toString(), urn2.toString());

    Urn urn3 = new TestEntityUrn("slice", "testUrn3", "VALUE_3");
    ObjectNode document3 = JsonNodeFactory.instance.objectNode();
    document3.set("urn", JsonNodeFactory.instance.textNode(urn3.toString()));
    document3.set("keyPart1", JsonNodeFactory.instance.textNode("slice_test_data"));
    document3.set("textFieldOverride", JsonNodeFactory.instance.textNode("slice_test_field3"));
    document3.set("browsePaths", JsonNodeFactory.instance.textNode("/slice/test/3"));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document3.toString(), urn3.toString());

    Urn urn4 = new TestEntityUrn("slice", "testUrn4", "VALUE_4");
    ObjectNode document4 = JsonNodeFactory.instance.objectNode();
    document4.set("urn", JsonNodeFactory.instance.textNode(urn4.toString()));
    document4.set("keyPart1", JsonNodeFactory.instance.textNode("slice_test_data"));
    document4.set("textFieldOverride", JsonNodeFactory.instance.textNode("slice_test_field4"));
    document4.set("browsePaths", JsonNodeFactory.instance.textNode("/slice/test/4"));
    elasticSearchService.upsertDocument(
        operationContext, ENTITY_NAME, document4.toString(), urn4.toString());

    syncAfterWrite(getBulkProcessor());
    clearCache();

    // Test without slice options - should return all results
    SearchResult searchResultAll =
        searchService.searchAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true).setSkipCache(true)),
            ImmutableList.of(),
            "slice_test_data",
            null,
            null,
            0,
            10,
            null);
    assertEquals(searchResultAll.getNumEntities().intValue(), 4);

    // Test slice fails
    SliceOptions slice0of2 = new SliceOptions().setId(0).setMax(2);
    assertThrows(
        IllegalStateException.class,
        () ->
            searchService.scrollAcrossEntities(
                operationContext.withSearchFlags(
                    flags -> flags.setFulltext(true).setSkipCache(true).setSliceOptions(slice0of2)),
                ImmutableList.of(),
                "slice_test_data",
                null,
                Collections.singletonList(SearchUtil.sortBy("urn", SortOrder.valueOf("ASCENDING"))),
                null,
                "10m",
                10,
                null));

    // Clean up test data
    elasticSearchService.deleteDocument(operationContext, ENTITY_NAME, urn1.toString());
    elasticSearchService.deleteDocument(operationContext, ENTITY_NAME, urn2.toString());
    elasticSearchService.deleteDocument(operationContext, ENTITY_NAME, urn3.toString());
    elasticSearchService.deleteDocument(operationContext, ENTITY_NAME, urn4.toString());
    syncAfterWrite(getBulkProcessor());

    // Verify cleanup
    ScrollResult searchResultAfterCleanup =
        searchService.scrollAcrossEntities(
            operationContext.withSearchFlags(flags -> flags.setFulltext(true).setSkipCache(true)),
            ImmutableList.of(),
            "slice_test_data",
            null,
            null,
            null,
            "10m",
            10,
            null);
    assertEquals(searchResultAfterCleanup.getNumEntities().intValue(), 0);

    clearCache();
  }
}
