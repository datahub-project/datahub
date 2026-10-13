package com.linkedin.metadata.search.elasticsearch;

import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class ElasticSearchServiceSplitClusterReindexTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoValidate();

  @Test
  public void testBuildReindexConfigsInspectsOwningCluster() throws Exception {
    ESIndexBuilder v2Builder = mock(ESIndexBuilder.class);
    ESIndexBuilder v3Builder = mock(ESIndexBuilder.class);
    stubBuilderConfig(v2Builder);
    stubBuilderConfig(v3Builder);

    MappingsBuilder.IndexMapping v2Mapping =
        MappingsBuilder.IndexMapping.builder()
            .indexName("datasetindex_v2")
            .mappings(Map.of())
            .build();
    MappingsBuilder.IndexMapping v3Mapping =
        MappingsBuilder.IndexMapping.builder()
            .indexName("datasetindex_v3")
            .mappings(Map.of())
            .build();

    MappingsBuilder mappingsBuilder = mock(MappingsBuilder.class);
    when(mappingsBuilder.getIndexMappings(any(OperationContext.class), any(Collection.class)))
        .thenReturn(List.of(v2Mapping, v3Mapping));

    SettingsBuilder settingsBuilder = mock(SettingsBuilder.class);
    when(settingsBuilder.getSettings(any(), any())).thenReturn(Map.of());

    ReindexConfig v2Config = mock(ReindexConfig.class);
    when(v2Config.name()).thenReturn("datasetindex_v2");
    ReindexConfig v3Config = mock(ReindexConfig.class);
    when(v3Config.name()).thenReturn("datasetindex_v3");
    when(v2Builder.buildReindexState(any(), eq("datasetindex_v2"), any(), any(), eq(false)))
        .thenReturn(v2Config);
    when(v3Builder.buildReindexState(any(), eq("datasetindex_v3"), any(), any(), eq(false)))
        .thenReturn(v3Config);

    ElasticSearchService service =
        new ElasticSearchService(
            v2Builder,
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mappingsBuilder,
            settingsBuilder,
            name -> name.contains("index_v3") ? v3Builder : v2Builder,
            mock(ESSearchDAO.class),
            mock(ESBrowseDAO.class),
            mock(ESWriteDAO.class));

    List<ReindexConfig> configs = service.buildReindexConfigs(OP_CONTEXT, List.of());
    assertEquals(configs, List.of(v2Config, v3Config));

    verify(v2Builder).buildReindexState(any(), eq("datasetindex_v2"), any(), any(), eq(false));
    verify(v3Builder).buildReindexState(any(), eq("datasetindex_v3"), any(), any(), eq(false));
    verify(v2Builder, never())
        .buildReindexState(any(), eq("datasetindex_v3"), any(), any(), eq(false));
    verify(v3Builder, never())
        .buildReindexState(any(), eq("datasetindex_v2"), any(), any(), eq(false));

    service.reindexAll(OP_CONTEXT, List.of());
    verify(v2Builder).buildIndex(OP_CONTEXT, v2Config);
    verify(v3Builder).buildIndex(OP_CONTEXT, v3Config);
    verify(v2Builder, never()).buildIndex(OP_CONTEXT, v3Config);
    verify(v3Builder, never()).buildIndex(OP_CONTEXT, v2Config);
  }

  @Test
  public void testUnrecognizedIndexNameIsAnError() {
    MappingsBuilder mappingsBuilder = mock(MappingsBuilder.class);
    when(mappingsBuilder.getIndexMappings(any(OperationContext.class), any(Collection.class)))
        .thenReturn(
            List.of(
                MappingsBuilder.IndexMapping.builder()
                    .indexName("graph_service_v1")
                    .mappings(Map.of())
                    .build()));

    ElasticSearchService failing =
        new ElasticSearchService(
            mock(ESIndexBuilder.class),
            TEST_SEARCH_SERVICE_CONFIG,
            mock(ElasticSearchConfiguration.class),
            mappingsBuilder,
            mock(SettingsBuilder.class),
            name -> {
              throw new IllegalArgumentException("unrecognized: " + name);
            },
            mock(ESSearchDAO.class),
            mock(ESBrowseDAO.class),
            mock(ESWriteDAO.class));

    assertThrows(
        IllegalArgumentException.class, () -> failing.buildReindexConfigs(OP_CONTEXT, List.of()));
  }

  private static void stubBuilderConfig(ESIndexBuilder builder) {
    ElasticSearchConfiguration config = mock(ElasticSearchConfiguration.class);
    when(config.getIndex()).thenReturn(mock(IndexConfiguration.class));
    when(builder.getConfig()).thenReturn(config);
  }
}
