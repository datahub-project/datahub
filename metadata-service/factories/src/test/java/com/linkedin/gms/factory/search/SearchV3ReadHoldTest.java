package com.linkedin.gms.factory.search;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.gms.factory.entity.update.indices.UpdateIndicesStrategyFactory;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexMigrationStamp;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SearchV3CutoverStamp;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.testng.annotations.Test;

public class SearchV3ReadHoldTest {

  @Test
  public void testNoV2IndexLeavesTheFlagsAlone() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    StandardEnvironment environment = new StandardEnvironment();
    hold(environment, false, null).applyTo(config);

    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    assertFalse(environment.getPropertySources().contains(SearchV3ReadHold.PROPERTY_SOURCE));
    assertFalse(UpdateIndicesStrategyFactory.liveV2Enabled(config));
  }

  @Test
  public void testIncompletePairHoldsReadsAndPublishesTheFlags() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    StandardEnvironment environment = new StandardEnvironment();
    hold(environment, true, Map.of()).applyTo(config);

    assertTrue(config.getEntityIndex().getV2().isEnabled());
    assertFalse(config.getEntityIndex().getV3().isKeywordReadEnabled());
    assertFalse(config.getEntityIndex().getV3().isHybridReadEnabled());
    assertEquals(environment.getProperty(SearchV3ReadHold.V2_ENABLED), "true");
    assertEquals(environment.getProperty(SearchV3ReadHold.V3_KEYWORD_READ), "false");
    assertEquals(environment.getProperty(SearchV3ReadHold.V3_HYBRID_READ), "false");
    assertTrue(UpdateIndicesStrategyFactory.liveV2Enabled(config));
  }

  @Test
  public void testCompletePairRetiresV2AndDoesNotHold() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    Stubbed stubbed =
        stub(null, true, Map.of("_meta", Map.of("migration", "complete")), Map.of(), List.of());
    stubbed.hold.applyTo(config);

    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    assertFalse(UpdateIndicesStrategyFactory.liveV2Enabled(config));
    verify(stubbed.v2Client).putIndexMapping(any(), any(PutMappingRequest.class), any());
  }

  @Test
  public void testRetiredV2StaysOnV3WhenTheMigrationMarkIsGone() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    Stubbed stubbed =
        stub(
            null,
            true,
            Map.of(),
            Map.of("_meta", Map.of(SearchV3CutoverStamp.KEY, SearchV3CutoverStamp.RETIRED)),
            List.of());
    stubbed.hold.applyTo(config);

    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    verify(stubbed.v2Client, never()).putIndexMapping(any(), any(), any());
  }

  @Test
  public void testUnreadableV3AfterCutoverStopsStartup() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    SearchV3ReadHold readHold =
        hold(
            null,
            true,
            null,
            Map.of("_meta", Map.of(SearchV3CutoverStamp.KEY, SearchV3CutoverStamp.RETIRED)));

    assertThrows(IllegalStateException.class, () -> readHold.applyTo(config));
    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
  }

  @Test
  public void testMissingV3AfterCutoverStopsStartup() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    Stubbed stubbed =
        stub(
            null,
            true,
            Map.of(),
            Map.of("_meta", Map.of(SearchV3CutoverStamp.KEY, SearchV3CutoverStamp.RETIRED)),
            List.of());
    when(stubbed.v3Client.indexExists(any(), any(), any())).thenReturn(false);

    assertThrows(IllegalStateException.class, () -> stubbed.hold.applyTo(config));
    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
  }

  @Test
  public void testMissingV3BeforeCutoverHolds() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    Stubbed stubbed = stub(null, true, Map.of(), Map.of(), List.of());
    when(stubbed.v3Client.indexExists(any(), any(), any())).thenReturn(false);

    stubbed.hold.applyTo(config);

    assertTrue(config.getEntityIndex().getV2().isEnabled());
    assertFalse(config.getEntityIndex().getV3().isKeywordReadEnabled());
    verify(stubbed.v2Client, never()).putIndexMapping(any(), any(), any());
  }

  @Test
  public void testRetiredMarkWriteFailureStopsStartup() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    Stubbed stubbed =
        stub(null, true, Map.of("_meta", Map.of("migration", "complete")), Map.of(), List.of());
    when(stubbed.v2Client.putIndexMapping(any(), any(), any()))
        .thenThrow(new IOException("rejected"));

    assertThrows(IllegalStateException.class, () -> stubbed.hold.applyTo(config));
    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
  }

  @Test
  public void testV2ListingFailureStopsStartup() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    SearchClusterRegistry registry = mock(SearchClusterRegistry.class);
    SearchClientShim<?> failing = mock(SearchClientShim.class);
    when(failing.indexExists(any(), any(), any())).thenThrow(new IOException("down"));
    doReturn(failing).when(registry).clientFor(SearchComponent.SEARCH_V2);
    doReturn(mock(SearchClientShim.class)).when(registry).clientFor(SearchComponent.SEARCH_V3);
    SearchV3ReadHold failingHold =
        new SearchV3ReadHold(registry, indexConvention(), entityRegistry(), null, false);

    assertThrows(IllegalStateException.class, () -> failingHold.applyTo(config));
    assertFalse(config.getEntityIndex().getV2().isEnabled());
  }

  @Test
  public void testNullV2BlockIsCreatedAndHeld() throws Exception {
    ElasticSearchConfiguration config =
        ElasticSearchConfiguration.builder()
            .entityIndex(
                EntityIndexConfiguration.builder()
                    .v3(
                        EntityIndexVersionConfiguration.builder()
                            .enabled(true)
                            .keywordReadEnabled(true)
                            .hybridReadEnabled(true)
                            .build())
                    .build())
            .build();
    hold(new StandardEnvironment(), true, Map.of()).applyTo(config);

    assertTrue(config.getEntityIndex().getV2().isEnabled());
    assertFalse(config.getEntityIndex().getV3().isKeywordReadEnabled());
  }

  @Test
  public void testHoldAlsoReachesAClusterCopyAndKeepsItsAnalyzer() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    EntityIndexConfiguration copy =
        EntityIndexConfiguration.builder()
            .v2(
                EntityIndexVersionConfiguration.builder()
                    .enabled(false)
                    .analyzerConfig("os3_analyzer.yaml")
                    .build())
            .v3(
                EntityIndexVersionConfiguration.builder()
                    .enabled(true)
                    .keywordReadEnabled(true)
                    .hybridReadEnabled(true)
                    .analyzerConfig("os3_analyzer.yaml")
                    .build())
            .build();
    SearchClusterRegistry.ClusterConnection secondary =
        new SearchClusterRegistry.ClusterConnection(
            "secondary",
            ElasticSearchConfiguration.builder().entityIndex(copy).build(),
            null,
            null,
            null);

    hold(new StandardEnvironment(), true, Map.of(), List.of(secondary)).applyTo(config);

    assertTrue(copy.getV2().isEnabled());
    assertFalse(copy.getV3().isKeywordReadEnabled());
    assertFalse(copy.getV3().isHybridReadEnabled());
    assertEquals(copy.getV2().getAnalyzerConfig(), "os3_analyzer.yaml");
    assertEquals(copy.getV3().getAnalyzerConfig(), "os3_analyzer.yaml");
  }

  @Test
  public void testHoldCreatesAMissingV3BlockOnAClusterCopy() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    EntityIndexConfiguration copy =
        EntityIndexConfiguration.builder()
            .v2(
                EntityIndexVersionConfiguration.builder()
                    .enabled(false)
                    .analyzerConfig("os3_analyzer.yaml")
                    .build())
            .build();
    SearchClusterRegistry.ClusterConnection secondary =
        new SearchClusterRegistry.ClusterConnection(
            "secondary",
            ElasticSearchConfiguration.builder().entityIndex(copy).build(),
            null,
            null,
            null);

    hold(new StandardEnvironment(), true, Map.of(), Map.of(), List.of(secondary)).applyTo(config);

    assertTrue(copy.getV2().isEnabled());
    assertEquals(copy.getV2().getAnalyzerConfig(), "os3_analyzer.yaml");
    assertFalse(copy.getV3().isKeywordReadEnabled());
    assertFalse(copy.getV3().isHybridReadEnabled());
  }

  @Test
  public void testAcknowledgedCutoverRetiresV2WithoutHolding() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    config.getEntityIndex().getV3().setCutoverAcknowledged(true);
    Stubbed stubbed = stub(null, true, Map.of(), Map.of(), List.of());

    stubbed.hold.applyTo(config);

    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    assertTrue(config.getEntityIndex().getV3().isHybridReadEnabled());
    verify(stubbed.v2Client).putIndexMapping(any(), any(PutMappingRequest.class), any());
  }

  @Test
  public void testAcknowledgementIgnoredWhenV3IsMissing() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    config.getEntityIndex().getV3().setCutoverAcknowledged(true);
    Stubbed stubbed = stub(null, true, Map.of(), Map.of(), List.of());
    when(stubbed.v3Client.indexExists(any(), any(), any())).thenReturn(false);

    stubbed.hold.applyTo(config);

    assertTrue(config.getEntityIndex().getV2().isEnabled());
    assertFalse(config.getEntityIndex().getV3().isKeywordReadEnabled());
    verify(stubbed.v2Client, never()).putIndexMapping(any(), any(), any());
  }

  @Test
  public void testAcknowledgementIgnoredWhenV3IsEmptyBesideV2() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    config.getEntityIndex().getV3().setCutoverAcknowledged(true);
    Stubbed stubbed = stub(null, true, Map.of(), Map.of(), List.of());
    CountResponse v2Documents = countOf(4);
    CountResponse noDocuments = countOf(0);
    when(stubbed.v2Client.count(any(), any(), any())).thenReturn(v2Documents);
    when(stubbed.v3Client.count(any(), any(), any())).thenReturn(noDocuments);

    stubbed.hold.applyTo(config);

    assertTrue(config.getEntityIndex().getV2().isEnabled());
    assertFalse(config.getEntityIndex().getV3().isKeywordReadEnabled());
    verify(stubbed.v2Client, never()).putIndexMapping(any(), any(), any());
  }

  @Test
  public void testAcknowledgementRetiresV2WhenV3HasFewerDocuments() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    config.getEntityIndex().getV3().setCutoverAcknowledged(true);
    Stubbed stubbed = stub(null, true, Map.of(), Map.of(), List.of());
    CountResponse v2Documents = countOf(4);
    CountResponse partialV3 = countOf(1);
    when(stubbed.v2Client.count(any(), any(), any())).thenReturn(v2Documents);
    when(stubbed.v3Client.count(any(), any(), any())).thenReturn(partialV3);

    stubbed.hold.applyTo(config);

    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    verify(stubbed.v2Client).putIndexMapping(any(), any(PutMappingRequest.class), any());
  }

  @Test
  public void testAcknowledgementRetiresV2WhenOneTypeHasFewerDocuments() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    config.getEntityIndex().getV3().setCutoverAcknowledged(true);
    Stubbed stubbed = twoEntityStub(false);
    when(stubbed.v3Client.indexExists(any(), any(), any())).thenReturn(true);
    CountResponse oneDocument = countOf(1);
    CountResponse fourDocuments = countOf(4);
    CountResponse fiveDocuments = countOf(5);
    when(stubbed.v2Client.count(any(), any(), any()))
        .thenAnswer(
            invocation ->
                "datasetindex_v2".equals(((CountRequest) invocation.getArgument(1)).indices()[0])
                    ? oneDocument
                    : fourDocuments);
    when(stubbed.v3Client.count(any(), any(), any()))
        .thenAnswer(
            invocation ->
                "datasetindex_v3".equals(((CountRequest) invocation.getArgument(1)).indices()[0])
                    ? fiveDocuments
                    : oneDocument);

    stubbed.hold.applyTo(config);

    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    verify(stubbed.v2Client, times(2)).putIndexMapping(any(), any(PutMappingRequest.class), any());
  }

  @Test
  public void testAcknowledgementRefusesWhenOneV3IsMissingAndAnotherHasDocuments()
      throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    config.getEntityIndex().getV3().setCutoverAcknowledged(true);
    Stubbed stubbed = twoEntityStub(false);

    assertThrows(IllegalStateException.class, () -> stubbed.hold.applyTo(config));
    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    verify(stubbed.v2Client, never()).putIndexMapping(any(), any(), any());
  }

  @Test
  public void testAcknowledgementContinuesWhenRebuildingAMissingPartner() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    config.getEntityIndex().getV3().setCutoverAcknowledged(true);
    Stubbed stubbed = twoEntityStub(true);

    stubbed.hold.applyTo(config);

    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    verify(stubbed.v2Client, never()).putIndexMapping(any(), any(), any());
  }

  @Test
  public void testAcknowledgementRefusesWhenACountFails() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    config.getEntityIndex().getV3().setCutoverAcknowledged(true);
    Stubbed stubbed = stub(null, true, Map.of(), Map.of(), List.of());
    when(stubbed.v3Client.count(any(), any(), any())).thenThrow(new IOException("count failed"));

    IllegalStateException refused =
        expectThrows(IllegalStateException.class, () -> stubbed.hold.applyTo(config));
    assertTrue(refused.getMessage().contains("Cannot count"));
    assertFalse(refused.getMessage().contains("cannot be read"));
    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    verify(stubbed.v2Client, never()).putIndexMapping(any(), any(), any());
  }

  @Test
  public void testAcknowledgementRetiresV2WhenEachEntityTypeIsCovered() throws Exception {
    for (boolean shared : new boolean[] {true, false}) {
      ElasticSearchConfiguration config = v3Reads();
      config.getEntityIndex().getV3().setCutoverAcknowledged(true);
      Stubbed stubbed = pairStub(shared, Map.of("dataset", 4L, "chart", 4L), false);

      stubbed.hold.applyTo(config);

      assertFalse(config.getEntityIndex().getV2().isEnabled(), "shared=" + shared);
      assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled(), "shared=" + shared);
      verify(stubbed.v2Client, times(2))
          .putIndexMapping(any(), any(PutMappingRequest.class), any());
    }
  }

  @Test
  public void testAcknowledgementRefusesWhenASharedIndexHoldsOnlyOneEntityType() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    config.getEntityIndex().getV3().setCutoverAcknowledged(true);
    Stubbed stubbed = pairStub(true, Map.of("dataset", 8L, "chart", 0L), false);

    assertThrows(IllegalStateException.class, () -> stubbed.hold.applyTo(config));
    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    verify(stubbed.v2Client, never()).putIndexMapping(any(), any(), any());
  }

  @Test
  public void testAcknowledgementRefusesWhenV2HasDocumentsButItsV3TypeIsEmpty() throws Exception {
    for (boolean shared : new boolean[] {true, false}) {
      ElasticSearchConfiguration config = v3Reads();
      config.getEntityIndex().getV3().setCutoverAcknowledged(true);
      Stubbed stubbed = pairStub(shared, Map.of("dataset", 8L, "chart", 0L), false);

      assertThrows(IllegalStateException.class, () -> stubbed.hold.applyTo(config));
      assertFalse(config.getEntityIndex().getV2().isEnabled(), "shared=" + shared);
      verify(stubbed.v2Client, never()).putIndexMapping(any(), any(), any());
    }
  }

  @Test
  public void testAcknowledgementRetiresV2WhenAnUnusedTypeHasNoV2Documents() throws Exception {
    for (boolean shared : new boolean[] {true, false}) {
      ElasticSearchConfiguration config = v3Reads();
      config.getEntityIndex().getV3().setCutoverAcknowledged(true);
      Stubbed stubbed = pairStub(shared, Map.of("dataset", 8L, "chart", 0L), false);
      countV2Total(stubbed.v2Client, Map.of("datasetindex_v2", 4L, "chartindex_v2", 0L));

      stubbed.hold.applyTo(config);

      assertFalse(config.getEntityIndex().getV2().isEnabled(), "shared=" + shared);
      assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled(), "shared=" + shared);
      verify(stubbed.v2Client, times(2))
          .putIndexMapping(any(), any(PutMappingRequest.class), any());
    }
  }

  @Test
  public void testAcknowledgementRetiresV2WhenEveryEntityTypeHasFewerDocuments() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    config.getEntityIndex().getV3().setCutoverAcknowledged(true);
    Stubbed stubbed = pairStub(true, Map.of("dataset", 1L, "chart", 1L), false);

    stubbed.hold.applyTo(config);

    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    verify(stubbed.v2Client, times(2)).putIndexMapping(any(), any(PutMappingRequest.class), any());
  }

  @Test
  public void testAcknowledgedCutoverTrustsARetiredStamp() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    config.getEntityIndex().getV3().setCutoverAcknowledged(true);
    Stubbed stubbed =
        stub(
            null,
            true,
            Map.of(),
            Map.of("_meta", Map.of(SearchV3CutoverStamp.KEY, SearchV3CutoverStamp.RETIRED)),
            List.of());
    CountResponse v2Documents = countOf(4);
    CountResponse partialV3 = countOf(1);
    when(stubbed.v2Client.count(any(), any(), any())).thenReturn(v2Documents);
    when(stubbed.v3Client.count(any(), any(), any())).thenReturn(partialV3);

    stubbed.hold.applyTo(config);

    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    verify(stubbed.v2Client, never()).putIndexMapping(any(), any(), any());
  }

  @Test
  public void testAcknowledgementStopsWhenTheV3IndexCannotBeListed() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    config.getEntityIndex().getV3().setCutoverAcknowledged(true);
    Stubbed stubbed = stub(null, true, Map.of(), Map.of(), List.of());
    when(stubbed.v3Client.indexExists(any(), any(), any())).thenThrow(new IOException("down"));

    IllegalStateException refused =
        expectThrows(IllegalStateException.class, () -> stubbed.hold.applyTo(config));
    assertTrue(refused.getMessage().contains("cannot be read"));
    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    verify(stubbed.v2Client, never()).putIndexMapping(any(), any(), any());
  }

  @Test
  public void testAcknowledgementStopsWhenTheV3MappingCannotBeRead() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    config.getEntityIndex().getV3().setCutoverAcknowledged(true);
    Stubbed stubbed = stub(null, true, null, Map.of(), List.of());

    IllegalStateException refused =
        expectThrows(IllegalStateException.class, () -> stubbed.hold.applyTo(config));
    assertTrue(refused.getMessage().contains("cannot be read"));
    assertFalse(config.getEntityIndex().getV2().isEnabled());
    verify(stubbed.v2Client, never()).putIndexMapping(any(), any(), any());
  }

  @Test
  public void testAcknowledgementStopsWhenOneV3CannotBeReadAndAnotherHasCaughtUp()
      throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    config.getEntityIndex().getV3().setCutoverAcknowledged(true);
    Stubbed stubbed = pairStub(false, Map.of("dataset", 4L, "chart", 4L), false);
    when(stubbed.v3Client.indexExists(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              GetIndexRequest request = invocation.getArgument(1);
              if ("chartindex_v3".equals(request.indices()[0])) {
                throw new IOException("down");
              }
              return true;
            });

    IllegalStateException refused =
        expectThrows(IllegalStateException.class, () -> stubbed.hold.applyTo(config));
    assertTrue(refused.getMessage().contains("cannot be read"));
    assertFalse(config.getEntityIndex().getV2().isEnabled());
    verify(stubbed.v2Client, never()).putIndexMapping(any(), any(), any());
  }

  @Test
  public void testAcknowledgementContinuesWithoutHoldWhenRebuildingAnUnreadableV3()
      throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    config.getEntityIndex().getV3().setCutoverAcknowledged(true);
    Stubbed stubbed = stub(null, true, null, Map.of(), List.of(), true);

    stubbed.hold.applyTo(config);

    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    verify(stubbed.v2Client, never()).putIndexMapping(any(), any(), any());
  }

  @Test
  public void testMissingV3AfterCutoverContinuesWhenRebuilding() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    Stubbed stubbed =
        stub(
            null,
            true,
            Map.of(),
            Map.of("_meta", Map.of(SearchV3CutoverStamp.KEY, SearchV3CutoverStamp.RETIRED)),
            List.of(),
            true);
    when(stubbed.v3Client.indexExists(any(), any(), any())).thenReturn(false);

    stubbed.hold.applyTo(config);

    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
    verify(stubbed.v2Client, never()).putIndexMapping(any(), any(), any());
  }

  @Test
  public void testEmptyIncompleteIndexAfterCutoverStopsReaders() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    Stubbed stubbed =
        stub(
            null,
            true,
            Map.of("_meta", Map.of("migration", IndexMigrationStamp.INCOMPLETE)),
            Map.of("_meta", Map.of(SearchV3CutoverStamp.KEY, SearchV3CutoverStamp.RETIRED)),
            List.of());
    CountResponse noDocuments = countOf(0);
    when(stubbed.v3Client.count(any(), any(), any())).thenReturn(noDocuments);

    assertThrows(IllegalStateException.class, () -> stubbed.hold.applyTo(config));
    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
  }

  @Test
  public void testIncompleteIndexWithDocumentsBeforeCutoverHolds() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    Stubbed stubbed =
        stub(
            null,
            true,
            Map.of("_meta", Map.of("migration", IndexMigrationStamp.INCOMPLETE)),
            Map.of(),
            List.of());

    stubbed.hold.applyTo(config);

    assertTrue(config.getEntityIndex().getV2().isEnabled());
    assertFalse(config.getEntityIndex().getV3().isKeywordReadEnabled());
  }

  @Test
  public void testIncompleteIndexWithDocumentsAfterCutoverStopsReaders() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    Stubbed stubbed =
        stub(
            null,
            true,
            Map.of("_meta", Map.of("migration", IndexMigrationStamp.INCOMPLETE)),
            Map.of("_meta", Map.of(SearchV3CutoverStamp.KEY, SearchV3CutoverStamp.RETIRED)),
            List.of());

    assertThrows(IllegalStateException.class, () -> stubbed.hold.applyTo(config));
    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
  }

  @Test
  public void testIncompleteIndexWhoseCountFailsAfterCutoverStopsReaders() throws Exception {
    ElasticSearchConfiguration config = v3Reads();
    Stubbed stubbed =
        stub(
            null,
            true,
            Map.of("_meta", Map.of("migration", IndexMigrationStamp.INCOMPLETE)),
            Map.of("_meta", Map.of(SearchV3CutoverStamp.KEY, SearchV3CutoverStamp.RETIRED)),
            List.of());
    when(stubbed.v3Client.count(any(), any(), any())).thenThrow(new IOException("count failed"));

    assertThrows(IllegalStateException.class, () -> stubbed.hold.applyTo(config));
    assertFalse(config.getEntityIndex().getV2().isEnabled());
    assertTrue(config.getEntityIndex().getV3().isKeywordReadEnabled());
  }

  @Test
  public void testConfiguredBooleanIgnoresTheHoldOverlay() throws Exception {
    StandardEnvironment environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource("configured", Map.of(SearchV3ReadHold.V2_ENABLED, "false")));
    hold(environment, true, Map.of()).applyTo(v3Reads());

    assertEquals(environment.getProperty(SearchV3ReadHold.V2_ENABLED), "true");
    assertFalse(SearchV3ReadHold.configuredBoolean(environment, SearchV3ReadHold.V2_ENABLED, true));
    assertEquals(environment.getProperty(SearchV3ReadHold.V2_ENABLED), "true");
  }

  private static ElasticSearchConfiguration v3Reads() {
    return ElasticSearchConfiguration.builder()
        .entityIndex(
            EntityIndexConfiguration.builder()
                .v2(EntityIndexVersionConfiguration.builder().enabled(false).build())
                .v3(
                    EntityIndexVersionConfiguration.builder()
                        .enabled(true)
                        .keywordReadEnabled(true)
                        .hybridReadEnabled(true)
                        .build())
                .build())
        .build();
  }

  private static SearchV3ReadHold hold(
      StandardEnvironment environment, boolean v2Exists, Map<String, Object> v3Mapping)
      throws Exception {
    return hold(environment, v2Exists, v3Mapping, Map.of(), List.of());
  }

  private static SearchV3ReadHold hold(
      StandardEnvironment environment,
      boolean v2Exists,
      Map<String, Object> v3Mapping,
      Map<String, Object> v2Mapping)
      throws Exception {
    return hold(environment, v2Exists, v3Mapping, v2Mapping, List.of());
  }

  private static SearchV3ReadHold hold(
      StandardEnvironment environment,
      boolean v2Exists,
      Map<String, Object> v3Mapping,
      Collection<SearchClusterRegistry.ClusterConnection> connections)
      throws Exception {
    return hold(environment, v2Exists, v3Mapping, Map.of(), connections);
  }

  /**
   * @param v3Mapping live V3 mapping; null means the mapping read fails
   */
  private static SearchV3ReadHold hold(
      StandardEnvironment environment,
      boolean v2Exists,
      Map<String, Object> v3Mapping,
      Map<String, Object> v2Mapping,
      Collection<SearchClusterRegistry.ClusterConnection> connections)
      throws Exception {
    return stub(environment, v2Exists, v3Mapping, v2Mapping, connections).hold;
  }

  private static Stubbed stub(
      StandardEnvironment environment,
      boolean v2Exists,
      Map<String, Object> v3Mapping,
      Map<String, Object> v2Mapping,
      Collection<SearchClusterRegistry.ClusterConnection> connections)
      throws Exception {
    return stub(environment, v2Exists, v3Mapping, v2Mapping, connections, false);
  }

  private static Stubbed stub(
      StandardEnvironment environment,
      boolean v2Exists,
      Map<String, Object> v3Mapping,
      Map<String, Object> v2Mapping,
      Collection<SearchClusterRegistry.ClusterConnection> connections,
      boolean rebuildMissingIndices)
      throws Exception {
    SearchClusterRegistry registry = mock(SearchClusterRegistry.class);
    when(registry.allConnections()).thenReturn(connections);
    SearchClientShim<?> v2Client = mock(SearchClientShim.class);
    SearchClientShim<?> v3Client = mock(SearchClientShim.class);
    when(v2Client.indexExists(any(), any(), any())).thenReturn(v2Exists);
    when(v3Client.indexExists(any(), any(), any())).thenReturn(v2Exists);
    GetMappingsResponse v2Mappings = mappings("datasetindex_v2", v2Mapping);
    when(v2Client.getIndexMapping(any(), any(), any())).thenReturn(v2Mappings);
    if (v3Mapping == null) {
      when(v3Client.getIndexMapping(any(), any(), any())).thenThrow(new IOException("unreadable"));
    } else {
      GetMappingsResponse v3Mappings = mappings("datasetindex_v3", v3Mapping);
      when(v3Client.getIndexMapping(any(), any(), any())).thenReturn(v3Mappings);
    }
    when(v2Client.putIndexMapping(any(), any(), any())).thenReturn(null);
    CountResponse oneDocument = countOf(1);
    when(v2Client.count(any(), any(), any())).thenReturn(oneDocument);
    when(v3Client.count(any(), any(), any())).thenReturn(oneDocument);
    doReturn(v2Client).when(registry).clientFor(SearchComponent.SEARCH_V2);
    doReturn(v3Client).when(registry).clientFor(SearchComponent.SEARCH_V3);
    return new Stubbed(
        new SearchV3ReadHold(
            registry, indexConvention(), entityRegistry(), environment, rebuildMissingIndices),
        v2Client,
        v3Client);
  }

  private static CountResponse countOf(long count) {
    CountResponse response = mock(CountResponse.class);
    when(response.getCount()).thenReturn(count);
    return response;
  }

  private static final class Stubbed {
    private final SearchV3ReadHold hold;
    private final SearchClientShim<?> v2Client;
    private final SearchClientShim<?> v3Client;

    private Stubbed(
        SearchV3ReadHold hold, SearchClientShim<?> v2Client, SearchClientShim<?> v3Client) {
      this.hold = hold;
      this.v2Client = v2Client;
      this.v3Client = v3Client;
    }
  }

  private static GetMappingsResponse mappings(String indexName, Map<String, Object> source) {
    GetMappingsResponse response = mock(GetMappingsResponse.class);
    MappingMetadata metadata = mock(MappingMetadata.class);
    when(metadata.getSourceAsMap()).thenReturn(source);
    when(response.mappings()).thenReturn(Map.of(indexName, metadata));
    return response;
  }

  private static IndexConvention indexConvention() {
    IndexConvention convention = mock(IndexConvention.class);
    when(convention.getEntityIndexName(any(), any())).thenReturn("datasetindex_v2");
    when(convention.getEntityIndexNameV3(any(), any())).thenReturn("datasetindex_v3");
    return convention;
  }

  private static EntityRegistry entityRegistry() {
    EntitySpec spec = mock(EntitySpec.class);
    when(spec.getName()).thenReturn("dataset");
    when(spec.getSearchGroup()).thenReturn("dataset");
    EntityRegistry registry = mock(EntityRegistry.class);
    when(registry.getEntitySpecs()).thenReturn(Map.of("dataset", spec));
    return registry;
  }

  /**
   * Dataset and chart, with four documents in each V2 index. They share one V3 index ({@code
   * primary}) or each has its own. Every V3 index exists, and its count answers per entity type.
   */
  private static Stubbed pairStub(
      boolean shared, Map<String, Long> v3CountsByEntityType, boolean rebuildMissingIndices)
      throws Exception {
    IndexConvention convention = mock(IndexConvention.class);
    when(convention.getEntityIndexName(any(), eq("dataset"))).thenReturn("datasetindex_v2");
    when(convention.getEntityIndexName(any(), eq("chart"))).thenReturn("chartindex_v2");
    when(convention.getEntityIndexNameV3(any(), eq("primary"))).thenReturn("primaryindex_v3");
    when(convention.getEntityIndexNameV3(any(), eq("dataset"))).thenReturn("datasetindex_v3");
    when(convention.getEntityIndexNameV3(any(), eq("chart"))).thenReturn("chartindex_v3");
    EntitySpec dataset = mock(EntitySpec.class);
    when(dataset.getName()).thenReturn("dataset");
    when(dataset.getSearchGroup()).thenReturn(shared ? "primary" : "dataset");
    EntitySpec chart = mock(EntitySpec.class);
    when(chart.getName()).thenReturn("chart");
    when(chart.getSearchGroup()).thenReturn(shared ? "primary" : "chart");
    EntityRegistry entities = mock(EntityRegistry.class);
    when(entities.getEntitySpecs()).thenReturn(Map.of("dataset", dataset, "chart", chart));

    SearchClusterRegistry registry = mock(SearchClusterRegistry.class);
    when(registry.allConnections()).thenReturn(List.of());
    SearchClientShim<?> v2Client = mock(SearchClientShim.class);
    SearchClientShim<?> v3Client = mock(SearchClientShim.class);
    when(v2Client.indexExists(any(), any(), any())).thenReturn(true);
    when(v3Client.indexExists(any(), any(), any())).thenReturn(true);
    GetMappingsResponse empty = mappings("index", Map.of());
    when(v2Client.getIndexMapping(any(), any(), any())).thenReturn(empty);
    when(v3Client.getIndexMapping(any(), any(), any())).thenReturn(empty);
    countV2Total(v2Client, Map.of("datasetindex_v2", 4L, "chartindex_v2", 4L));
    countByEntityType(v3Client, v3CountsByEntityType);
    doReturn(v2Client).when(registry).clientFor(SearchComponent.SEARCH_V2);
    doReturn(v3Client).when(registry).clientFor(SearchComponent.SEARCH_V3);
    return new Stubbed(
        new SearchV3ReadHold(registry, convention, entities, null, rebuildMissingIndices),
        v2Client,
        v3Client);
  }

  /**
   * A count filtered on {@code _entityType} answers that type's count. An unfiltered count answers
   * the total, the shape of the whole-index count startup also reads.
   */
  private static void countByEntityType(SearchClientShim<?> client, Map<String, Long> counts)
      throws IOException {
    Map<String, CountResponse> responses = new HashMap<>();
    counts.forEach((entityType, count) -> responses.put(entityType, countOf(count)));
    CountResponse none = countOf(0);
    CountResponse total = countOf(counts.values().stream().mapToLong(Long::longValue).sum());
    doAnswer(
            invocation -> {
              QueryBuilder query = ((CountRequest) invocation.getArgument(1)).query();
              if (query == null) {
                return total;
              }
              TermQueryBuilder term = (TermQueryBuilder) query;
              assertEquals(term.fieldName(), MappingConstants.ENTITY_TYPE_FIELD);
              return responses.getOrDefault((String) term.value(), none);
            })
        .when(client)
        .count(any(), any(), any());
  }

  /**
   * A V2 index holds one entity type and stores no _entityType field, so a term query on it finds
   * nothing. Only the unfiltered count answers with the index's documents. A term query that
   * answered with a real count would let a regression that counts V2 by _entityType pass.
   */
  private static void countV2Total(SearchClientShim<?> client, Map<String, Long> totalByIndex)
      throws IOException {
    CountResponse none = countOf(0);
    doAnswer(
            invocation -> {
              CountRequest request = invocation.getArgument(1);
              if (request.query() != null) {
                return none;
              }
              return countOf(totalByIndex.getOrDefault(request.indices()[0], 0L));
            })
        .when(client)
        .count(any(), any(), any());
  }

  /** Dataset's V3 index has documents. Chart's V3 index is missing. Both V2 indices exist. */
  private static Stubbed twoEntityStub(boolean rebuildMissingIndices) throws Exception {
    IndexConvention convention = mock(IndexConvention.class);
    when(convention.getEntityIndexName(any(), eq("dataset"))).thenReturn("datasetindex_v2");
    when(convention.getEntityIndexName(any(), eq("chart"))).thenReturn("chartindex_v2");
    when(convention.getEntityIndexNameV3(any(), eq("dataset"))).thenReturn("datasetindex_v3");
    when(convention.getEntityIndexNameV3(any(), eq("chart"))).thenReturn("chartindex_v3");
    EntitySpec dataset = mock(EntitySpec.class);
    when(dataset.getName()).thenReturn("dataset");
    when(dataset.getSearchGroup()).thenReturn("dataset");
    EntitySpec chart = mock(EntitySpec.class);
    when(chart.getName()).thenReturn("chart");
    when(chart.getSearchGroup()).thenReturn("chart");
    EntityRegistry entities = mock(EntityRegistry.class);
    when(entities.getEntitySpecs()).thenReturn(Map.of("dataset", dataset, "chart", chart));

    SearchClusterRegistry registry = mock(SearchClusterRegistry.class);
    when(registry.allConnections()).thenReturn(List.of());
    SearchClientShim<?> v2Client = mock(SearchClientShim.class);
    SearchClientShim<?> v3Client = mock(SearchClientShim.class);
    when(v2Client.indexExists(any(), any(), any())).thenReturn(true);
    when(v3Client.indexExists(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              GetIndexRequest request = invocation.getArgument(1);
              return request != null && "datasetindex_v3".equals(request.indices()[0]);
            });
    GetMappingsResponse empty = mappings("index", Map.of());
    when(v2Client.getIndexMapping(any(), any(), any())).thenReturn(empty);
    when(v3Client.getIndexMapping(any(), any(), any())).thenReturn(empty);
    CountResponse oneDocument = countOf(1);
    when(v2Client.count(any(), any(), any())).thenReturn(oneDocument);
    when(v3Client.count(any(), any(), any())).thenReturn(oneDocument);
    doReturn(v2Client).when(registry).clientFor(SearchComponent.SEARCH_V2);
    doReturn(v3Client).when(registry).clientFor(SearchComponent.SEARCH_V3);
    return new Stubbed(
        new SearchV3ReadHold(registry, convention, entities, null, rebuildMissingIndices),
        v2Client,
        v3Client);
  }
}
