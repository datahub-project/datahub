package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.testng.annotations.Test;

public class MigrationFamilyTest {
  private static final IndexConvention CONVENTION =
      IndexConventionImpl.noPrefix("MD5", new EntityIndexConfiguration());

  private static MigrationFamily family(SearchComponent family) {
    return MigrationFamily.of(
        family,
        mock(ElasticSearchGraphService.class),
        mock(ElasticSearchSystemMetadataService.class),
        mock(ElasticSearchTimeseriesAspectService.class));
  }

  @Test
  public void testAFamilyOnAnotherBackendIsBuiltButCannotMigrate() {
    // A Neo4j graph: the family exists, so the upgrade comes up on such a deployment, and only a
    // migration configured for it fails.
    MigrationFamily graphOnNeo4j =
        MigrationFamily.of(
            SearchComponent.GRAPH,
            mock(GraphService.class),
            mock(ElasticSearchSystemMetadataService.class),
            mock(ElasticSearchTimeseriesAspectService.class));

    assertFalse(graphOnNeo4j.isSupported());
    assertThrows(IllegalStateException.class, graphOnNeo4j::checkSupported);
    assertTrue(family(SearchComponent.GRAPH).isSupported());
    assertTrue(family(SearchComponent.USAGE).isSupported(), "usage events have no service");
  }

  @Test
  public void testOnlyUsageEventsAreWrittenOnce() {
    // Every other family's documents are updated in place, so a windowed pass must replace them.
    assertTrue(family(SearchComponent.GRAPH).isMutable());
    assertTrue(family(SearchComponent.SYSTEM_METADATA).isMutable());
    assertTrue(family(SearchComponent.TIMESERIES).isMutable());
    assertFalse(family(SearchComponent.USAGE).isMutable());
  }

  @Test
  public void testTheGraphWindowLooksAtBothTimestampsAndOthersAtOne() {
    ObjectMapper mapper = new ObjectMapper();

    ObjectNode graph = family(SearchComponent.GRAPH).windowFilter(mapper, 100L, null);
    assertEquals(graph.path("bool").path("minimum_should_match").asInt(), 1);
    assertEquals(
        graph
            .path("bool")
            .path("should")
            .get(0)
            .path("range")
            .path("createdOn")
            .path("gte")
            .asLong(),
        100L);
    assertEquals(
        graph
            .path("bool")
            .path("should")
            .get(1)
            .path("range")
            .path("updatedOn")
            .path("gte")
            .asLong(),
        100L);

    ObjectNode timeseries = family(SearchComponent.TIMESERIES).windowFilter(mapper, 100L, null);
    assertEquals(timeseries.path("range").path("@timestamp").path("gte").asLong(), 100L);
    assertFalse(timeseries.path("range").path("@timestamp").has("lte"));

    ObjectNode bounded = family(SearchComponent.SYSTEM_METADATA).windowFilter(mapper, 100L, 200L);
    assertEquals(bounded.path("range").path("lastUpdated").path("gte").asLong(), 100L);
    assertEquals(bounded.path("range").path("lastUpdated").path("lte").asLong(), 200L);
  }

  @Test
  public void testIndexNamesComeFromTheRegistryAndTheConventionAlone() {
    // No cluster client exists here at all: a name must not cost a round trip.
    OperationContext opContext = mock(OperationContext.class);
    when(opContext.getEntityRegistry()).thenReturn(TestOperationContexts.defaultEntityRegistry());

    assertEquals(
        family(SearchComponent.GRAPH).logicalNames(opContext, CONVENTION),
        List.of("graph_service_v1"));
    assertEquals(
        family(SearchComponent.SYSTEM_METADATA).logicalNames(opContext, CONVENTION),
        List.of("system_metadata_service_v1"));
    List<String> timeseries =
        family(SearchComponent.TIMESERIES).logicalNames(opContext, CONVENTION);
    assertTrue(
        timeseries.contains(
            CONVENTION.getTimeseriesAspectIndexName(opContext, "dataset", "datasetProfile")),
        timeseries.toString());
    assertTrue(timeseries.size() > 10, timeseries.toString());
  }
}
