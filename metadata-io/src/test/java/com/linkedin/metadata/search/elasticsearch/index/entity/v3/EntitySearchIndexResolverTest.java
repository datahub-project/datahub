package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import java.util.List;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.testng.annotations.Test;

public class EntitySearchIndexResolverTest {

  @Test
  public void shouldReadV3WhenV3Only() {
    EntityIndexConfiguration cfg =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(false).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(true).build())
            .build();
    assertTrue(EntitySearchIndexResolver.shouldReadV3(cfg));
  }

  @Test
  public void shouldNotReadV3DuringDualWriteWithoutKeywordRead() {
    EntityIndexConfiguration cfg =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(true).build())
            .build();
    assertFalse(EntitySearchIndexResolver.shouldReadV3(cfg));
  }

  @Test
  public void shouldReadV3WhenKeywordReadEnabled() {
    EntityIndexConfiguration cfg =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
            .v3(
                EntityIndexVersionConfiguration.builder()
                    .enabled(true)
                    .keywordReadEnabled(true)
                    .build())
            .build();
    assertTrue(EntitySearchIndexResolver.shouldReadV3(cfg));
  }

  @Test
  public void indexNamesDedupesConsolidatedGroup() {
    EntitySpec dataset = mock(EntitySpec.class);
    when(dataset.getName()).thenReturn("dataset");
    when(dataset.getSearchGroup()).thenReturn("primary");
    EntitySpec chart = mock(EntitySpec.class);
    when(chart.getName()).thenReturn("chart");
    when(chart.getSearchGroup()).thenReturn("primary");

    EntityRegistry registry = mock(EntityRegistry.class);
    when(registry.getEntitySpec("dataset")).thenReturn(dataset);
    when(registry.getEntitySpec("chart")).thenReturn(chart);

    IndexConvention convention = mock(IndexConvention.class);
    OperationContext opContext = mock(OperationContext.class);
    SearchContext searchContext = mock(SearchContext.class);
    when(opContext.getEntityRegistry()).thenReturn(registry);
    when(opContext.getSearchContext()).thenReturn(searchContext);
    when(searchContext.getIndexConvention()).thenReturn(convention);
    when(convention.getEntityIndexNameV3(opContext, "primary")).thenReturn("primaryindex_v3");

    EntityIndexConfiguration cfg =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(false).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(true).build())
            .build();

    String[] names =
        EntitySearchIndexResolver.indexNames(opContext, List.of("dataset", "chart"), cfg);
    assertEquals(names, new String[] {"primaryindex_v3"});
  }

  @Test
  public void indexNamesUsesEntityNameWhenSearchGroupUnset() {
    EntitySpec dataset = mock(EntitySpec.class);
    when(dataset.getName()).thenReturn("dataset");
    when(dataset.getSearchGroup()).thenReturn(null);

    EntityRegistry registry = mock(EntityRegistry.class);
    when(registry.getEntitySpec("dataset")).thenReturn(dataset);

    IndexConvention convention = mock(IndexConvention.class);
    OperationContext opContext = mock(OperationContext.class);
    SearchContext searchContext = mock(SearchContext.class);
    when(opContext.getEntityRegistry()).thenReturn(registry);
    when(opContext.getSearchContext()).thenReturn(searchContext);
    when(searchContext.getIndexConvention()).thenReturn(convention);
    when(convention.getEntityIndexNameV3(opContext, "dataset")).thenReturn("datasetindex_v3");

    EntityIndexConfiguration cfg =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(false).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(true).build())
            .build();

    assertEquals(EntitySearchIndexResolver.indexName(opContext, "dataset", cfg), "datasetindex_v3");
  }

  @Test
  public void entityTypeFilterAppliedOnV3EvenForEntityNamedIndex() {
    EntityIndexConfiguration cfg =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(false).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(true).build())
            .build();
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    EntitySearchIndexResolver.applyEntityTypeFilter(query, List.of("dataset"), cfg);

    assertEquals(query.filter().size(), 1);
    TermsQueryBuilder terms = (TermsQueryBuilder) query.filter().get(0);
    assertEquals(terms.fieldName(), "_entityType");
    assertTrue(terms.values().contains("dataset"));
  }

  @Test
  public void entityTypeFilterSkippedOnV2() {
    EntityIndexConfiguration cfg =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(false).build())
            .build();
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    EntitySearchIndexResolver.applyEntityTypeFilter(query, List.of("dataset"), cfg);
    assertTrue(query.filter().isEmpty());
  }

  @Test
  public void allEntityIndexPatternUsesV2WildcardWhenKeywordReadOff() {
    IndexConvention convention = mock(IndexConvention.class);
    OperationContext opContext = mock(OperationContext.class);
    SearchContext searchContext = mock(SearchContext.class);
    when(opContext.getSearchContext()).thenReturn(searchContext);
    when(searchContext.getIndexConvention()).thenReturn(convention);
    when(convention.getEntityIndexName(opContext, "*")).thenReturn("*index_v2");

    EntityIndexConfiguration cfg =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(true).build())
            .build();
    assertEquals(EntitySearchIndexResolver.allEntityIndexPattern(opContext, cfg), "*index_v2");
  }

  @Test
  public void allEntityIndexPatternUsesV3WildcardWhenKeywordReadOn() {
    IndexConvention convention = mock(IndexConvention.class);
    OperationContext opContext = mock(OperationContext.class);
    SearchContext searchContext = mock(SearchContext.class);
    when(opContext.getSearchContext()).thenReturn(searchContext);
    when(searchContext.getIndexConvention()).thenReturn(convention);
    when(convention.getV3EntityIndexPatterns(opContext)).thenReturn(List.of("*index_v3"));

    EntityIndexConfiguration cfg =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
            .v3(
                EntityIndexVersionConfiguration.builder()
                    .enabled(true)
                    .keywordReadEnabled(true)
                    .build())
            .build();
    assertEquals(EntitySearchIndexResolver.allEntityIndexPattern(opContext, cfg), "*index_v3");
  }

  @Test
  public void entityTypeFilterSkippedWhenEntityNamesEmpty() {
    EntityIndexConfiguration cfg =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(false).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(true).build())
            .build();
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    EntitySearchIndexResolver.applyEntityTypeFilter(query, List.of(), cfg);
    assertTrue(query.filter().isEmpty());
  }
}
