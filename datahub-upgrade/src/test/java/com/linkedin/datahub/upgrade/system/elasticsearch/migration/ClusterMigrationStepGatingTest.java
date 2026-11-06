package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeReport;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.ClusterMigrationConfiguration;
import com.linkedin.metadata.config.search.ComponentClusterConfiguration;
import com.linkedin.metadata.config.search.CopyMode;
import com.linkedin.metadata.config.search.CopySettings;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.FamilyMigrationSettings;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.config.search.SearchClusterSettings;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ClusterMigrationState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** The decisions the steps make before touching any cluster. */
public class ClusterMigrationStepGatingTest {
  private static final IndexConvention CONVENTION =
      IndexConventionImpl.noPrefix("MD5", new EntityIndexConfiguration());
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private OperationContext opContext;
  private ClusterMigrationHarness.InMemoryUpgradeRows rows;
  private EntityService<?> entityService;
  private SearchClientShim<?> sourceClient;
  private SearchClientShim<?> targetClient;
  private MigrationFamily graph;
  private MigrationFamily usage;
  private UpgradeContext context;

  @BeforeMethod
  public void setUp() throws Exception {
    rows = new ClusterMigrationHarness.InMemoryUpgradeRows();
    entityService = rows.entityService();
    sourceClient = mock(SearchClientShim.class);
    targetClient = mock(SearchClientShim.class);
    when(sourceClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    when(targetClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    opContext = mock(OperationContext.class);
    when(opContext.getObjectMapper()).thenReturn(MAPPER);
    graph = family(SearchComponent.GRAPH);
    usage = family(SearchComponent.USAGE);
    context = mock(UpgradeContext.class);
    when(context.opContext()).thenReturn(opContext);
    when(context.report()).thenReturn(new DefaultUpgradeReport());
  }

  private static MigrationFamily family(SearchComponent family) {
    return MigrationFamily.of(
        family,
        mock(ElasticSearchGraphService.class),
        mock(ElasticSearchSystemMetadataService.class),
        mock(ElasticSearchTimeseriesAspectService.class));
  }

  /** A registry with one family migrating from "primary" to "secondary", or none at all. */
  private SearchClusterRegistry registry(
      @Nullable SearchComponent migrating, boolean copyEnabled, int liveWriterWaitMinutes) {
    Map<String, SearchClusterSettings> clusters = new LinkedHashMap<>();
    clusters.put("primary", SearchClusterSettings.builder().uri("http://a:9200").build());
    clusters.put("secondary", SearchClusterSettings.builder().uri("http://b:9200").build());
    ClusterMigrationConfiguration.ClusterMigrationConfigurationBuilder migration =
        ClusterMigrationConfiguration.builder().liveWriterWaitMinutes(liveWriterWaitMinutes);
    if (migrating != null) {
      FamilyMigrationSettings settings =
          FamilyMigrationSettings.builder()
              .targetCluster("secondary")
              .copy(CopySettings.builder().enabled(copyEnabled).build())
              .build();
      switch (migrating) {
        case GRAPH -> migration.graph(settings);
        case USAGE -> migration.usage(settings);
        default -> throw new IllegalArgumentException(migrating.name());
      }
    }
    ElasticSearchConfiguration config =
        ElasticSearchConfiguration.builder()
            .clusters(clusters)
            .componentCluster(ComponentClusterConfiguration.builder().build())
            .clusterMigration(migration.build())
            .index(IndexConfiguration.builder().numShards(1).numReplicas(0).build())
            .build();
    Map<String, SearchClusterRegistry.ClusterConnection> connections = new LinkedHashMap<>();
    connections.put(
        "primary",
        new SearchClusterRegistry.ClusterConnection(
            "primary",
            config,
            sourceClient,
            mock(ESBulkProcessor.class),
            mock(ESIndexBuilder.class)));
    connections.put(
        "secondary",
        new SearchClusterRegistry.ClusterConnection(
            "secondary",
            config,
            targetClient,
            mock(ESBulkProcessor.class),
            mock(ESIndexBuilder.class)));
    return new SearchClusterRegistry(config, connections, CONVENTION, null);
  }

  @Test
  public void testStepsSkipFamiliesWithoutAMigration() {
    SearchClusterRegistry registry = registry(null, true, 0);
    assertTrue(
        new ClusterMigrationPrepareStep(opContext, entityService, registry, CONVENTION, graph, "v1")
            .skip(context));
    assertTrue(
        new ClusterMigrationCopyStep(opContext, entityService, registry, CONVENTION, graph, "v1")
            .skip(context));
  }

  @Test
  public void testCopyFailsLoudlyWhenPrepareNeverRan() throws Exception {
    ClusterMigrationCopyStep step =
        new ClusterMigrationCopyStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, true, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.FAILED);
    verify(sourceClient, never()).performLowLevelRequest(any(), any());
  }

  @Test
  public void testAFamilyOnAnotherBackendComesUpAndOnlyAConfiguredMigrationOfItFails()
      throws Exception {
    // A Neo4j graph: system update must still construct (such deployments run it with no migration
    // at all), and only a migration configured for that family fails, before it touches anything.
    GraphService neo4j = mock(GraphService.class);
    ClusterMigrationPrepare upgrade =
        new ClusterMigrationPrepare(
            opContext,
            entityService,
            registry(null, true, 0),
            CONVENTION,
            neo4j,
            mock(ElasticSearchSystemMetadataService.class),
            mock(ElasticSearchTimeseriesAspectService.class),
            "v1");
    assertEquals(upgrade.steps().size(), ClusterMigrationConfiguration.MIGRATABLE_FAMILIES.size());
    assertTrue(upgrade.steps().stream().allMatch(step -> step.skip(context)));

    MigrationFamily graphOnNeo4j =
        MigrationFamily.of(
            SearchComponent.GRAPH,
            neo4j,
            mock(ElasticSearchSystemMetadataService.class),
            mock(ElasticSearchTimeseriesAspectService.class));
    SearchClusterRegistry migrating = registry(SearchComponent.GRAPH, true, 0);
    ClusterMigrationPrepareStep prepare =
        new ClusterMigrationPrepareStep(
            opContext, entityService, migrating, CONVENTION, graphOnNeo4j, "v1");
    ClusterMigrationCopyStep copy =
        new ClusterMigrationCopyStep(
            opContext, entityService, migrating, CONVENTION, graphOnNeo4j, "v1");

    assertFalse(prepare.skip(context));
    assertEquals(prepare.executable().apply(context).result(), DataHubUpgradeState.FAILED);
    assertEquals(copy.executable().apply(context).result(), DataHubUpgradeState.FAILED);
    verify(targetClient, never()).performLowLevelRequest(any(), any());
    verify(sourceClient, never()).performLowLevelRequest(any(), any());
  }

  @Test
  public void testARejectedLogIndexMappingFailsThePrepare() throws Exception {
    // Only a creation that raced this one is tolerated; a rejected mapping would leave the family
    // without its delete log.
    StubCluster target = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    target.on("HEAD", "/graph_service_v1", new StubCluster.Reply(404, ""));
    target.on(
        "PUT",
        "/cluster_migration_log_graph",
        new StubCluster.Reply(
            400,
            "{\"error\":{\"type\":\"mapper_parsing_exception\",\"reason\":\"no such type\"}}"));
    targetClient = target.shim;
    ClusterMigrationPrepareStep step =
        new ClusterMigrationPrepareStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, true, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.FAILED);

    StubCluster raced = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    raced.on("HEAD", "/graph_service_v1", new StubCluster.Reply(404, ""));
    raced.on(
        "PUT",
        "/cluster_migration_log_graph",
        new StubCluster.Reply(400, "{\"error\":{\"type\":\"resource_already_exists_exception\"}}"));
    targetClient = raced.shim;
    ClusterMigrationPrepareStep racedStep =
        new ClusterMigrationPrepareStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, true, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(racedStep.executable().apply(context).result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testAWindowedPassThatWroteAnythingRunsAgainAndOnlyACleanOneVerifies()
      throws Exception {
    // The first pass finds a document the mirror had not delivered (a pod of the previous rollout
    // was still serving the old configuration when the copy began); the second finds the target
    // current, and only then is the family verified.
    StubCluster source = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    StubCluster target = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    source.on("HEAD", "/graph_service_v1", StubCluster.Reply.ok(""));
    source.on("POST", "/graph_service_v1/_refresh", StubCluster.Reply.ok("{}"));
    source.on("POST", "/graph_service_v1/_count", StubCluster.Reply.ok("{\"count\":1}"));
    source.on(
        "POST",
        "/graph_service_v1/_search",
        call ->
            call.body().contains("\"_source\":false")
                // The verifier's sample of the newest documents.
                ? StubCluster.Reply.ok("{\"hits\":{\"hits\":[]}}")
                // The windowed pass's one page.
                : StubCluster.Reply.ok(
                    "{\"_scroll_id\":\"s\",\"hits\":{\"total\":{\"value\":1},"
                        + "\"hits\":[{\"_id\":\"a\",\"_source\":{\"updatedOn\":5}}]}}"));
    source.on(
        "POST",
        "/_search/scroll",
        StubCluster.Reply.ok("{\"_scroll_id\":\"s\",\"hits\":{\"hits\":[]}}"));
    source.on("DELETE", "/_search/scroll", StubCluster.Reply.ok("{}"));
    target.on("POST", "/graph_service_v1/_refresh", StubCluster.Reply.ok("{}"));
    target.on("POST", "/graph_service_v1/_count", StubCluster.Reply.ok("{\"count\":1}"));
    List<String> bulkReplies =
        new ArrayList<>(
            List.of(
                "{\"items\":[{\"update\":{\"status\":200,\"result\":\"updated\"}}]}",
                "{\"items\":[{\"update\":{\"status\":200,\"result\":\"noop\"}}]}"));
    target.on("POST", "/_bulk", call -> StubCluster.Reply.ok(bulkReplies.remove(0)));
    sourceClient = source.shim;
    targetClient = target.shim;
    ClusterMigrationCopyStep.windowRepeatPauseMs = 0;
    String upgradeId =
        ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary");
    ClusterMigrationRow row = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    row.setStatus(ClusterMigrationState.Status.WINDOW);
    row.put(ClusterMigrationState.COPY_START_TIME, 100L);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS), 5L);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.MAE), 7L);
    row.persist(DataHubUpgradeState.IN_PROGRESS);
    ClusterMigrationCopyStep step =
        new ClusterMigrationCopyStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, true, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.SUCCEEDED);

    assertEquals(
        target.calls("POST", "/_bulk").size(),
        2,
        "the pass that wrote something was followed by one that wrote nothing");
    assertEquals(
        ClusterMigrationRow.load(opContext, entityService, upgradeId).status().orElseThrow(),
        ClusterMigrationState.Status.VERIFIED);
  }

  @Test
  public void testCopyWaitsForBothLiveWritersBeforeReadingTheSource() throws Exception {
    ClusterMigrationRow row =
        ClusterMigrationRow.load(
            opContext,
            entityService,
            ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary"));
    row.setStatus(ClusterMigrationState.Status.PREPARED);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS), 5L);
    row.persist(DataHubUpgradeState.IN_PROGRESS);
    ClusterMigrationCopyStep step =
        new ClusterMigrationCopyStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, true, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.SUCCEEDED);

    verify(sourceClient, never()).performLowLevelRequest(any(), any());
    verify(targetClient, never()).performLowLevelRequest(any(), any());
    assertEquals(
        ClusterMigrationRow.load(opContext, entityService, row.getUrn().getId())
            .status()
            .orElseThrow(),
        ClusterMigrationState.Status.PREPARED);
  }

  @Test
  public void testTheCopyWaitsWhileAWriterOfThePreviousRolloutStillReports() throws Exception {
    // Both markers of this rollout are in, but a pod of the previous rollout (started before the
    // stamp, serving the old configuration, so not mirroring) is still reporting: nothing may be
    // read from the source until it stops.
    sourceClient = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_2).shim;
    String upgradeId =
        ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary");
    ClusterMigrationRow row = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    row.setStatus(ClusterMigrationState.Status.PREPARED);
    row.put(ClusterMigrationState.ROLLOUT_STARTED_AT, 100L);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS), 150L);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.MAE), 160L);
    row.persist(DataHubUpgradeState.IN_PROGRESS);
    long now = System.currentTimeMillis();
    ClusterMigrationRow writers =
        ClusterMigrationRow.load(
            opContext, entityService, ClusterMigrationState.WRITERS_UPGRADE_ID);
    writers.put(
        ClusterMigrationState.writerKey("gms-old"), ClusterMigrationState.writerValue(50L, now));
    writers.put(
        ClusterMigrationState.writerKey("gms-new"), ClusterMigrationState.writerValue(150L, now));
    writers.persist(DataHubUpgradeState.SUCCEEDED);
    ClusterMigrationCopyStep step =
        new ClusterMigrationCopyStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, true, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.SUCCEEDED);

    assertEquals(
        ClusterMigrationRow.load(opContext, entityService, upgradeId).status().orElseThrow(),
        ClusterMigrationState.Status.PREPARED,
        "nothing started while the old pod was still writing");
    verify(sourceClient, never()).performLowLevelRequest(any(), any());

    // The old pod terminated: three missed reports later it no longer counts.
    writers =
        ClusterMigrationRow.load(
            opContext, entityService, ClusterMigrationState.WRITERS_UPGRADE_ID);
    writers.put(
        ClusterMigrationState.writerKey("gms-old"),
        ClusterMigrationState.writerValue(
            50L, now - ClusterMigrationState.WRITER_STALE_AFTER_MS - 1));
    writers.persist(DataHubUpgradeState.SUCCEEDED);

    step.executable().apply(context);

    assertNotEquals(
        ClusterMigrationRow.load(opContext, entityService, upgradeId).status().orElseThrow(),
        ClusterMigrationState.Status.PREPARED,
        "the copy started once the previous rollout's writers were gone");
  }

  @Test
  public void testCopyPicksUpTheLiveWritersWhileItWaits() throws Exception {
    // A source that answers (nothing exists on it), so the step gets past the wait and moves the
    // row on; the mocked target then stops it.
    sourceClient = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_2).shim;
    String upgradeId =
        ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary");
    ClusterMigrationRow row = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    row.setStatus(ClusterMigrationState.Status.PREPARED);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS), 5L);
    row.persist(DataHubUpgradeState.IN_PROGRESS);
    ClusterMigrationCopyStep step =
        new ClusterMigrationCopyStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, true, 1),
            CONVENTION,
            graph,
            "v1");
    // The MAE consumer finishes rolling while the step is already polling for it.
    Thread maeConsumer =
        new Thread(
            () -> {
              try {
                Thread.sleep(500);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
              ClusterMigrationRow late =
                  ClusterMigrationRow.load(opContext, entityService, upgradeId);
              late.put(
                  ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.MAE),
                  7L);
              late.persist(DataHubUpgradeState.IN_PROGRESS);
            });
    maeConsumer.start();

    step.executable().apply(context);
    maeConsumer.join();

    ClusterMigrationRow after = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    assertNotEquals(
        after.status().orElseThrow(),
        ClusterMigrationState.Status.PREPARED,
        "the copy started once both writers had reported");
    assertEquals(
        after.get(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.MAE)),
        "7",
        "what the step persisted after the wait keeps the late marker");
  }

  @Test
  public void testCopyDropsCursorsWrittenByAnotherCopier() throws Exception {
    // A remote reindex attempt left its task ids as cursors; the deployment now copies in process.
    String upgradeId =
        ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary");
    ClusterMigrationRow row = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    row.setStatus(ClusterMigrationState.Status.FAILED);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS), 5L);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.MAE), 7L);
    row.put(ClusterMigrationState.COPY_MODE, CopyMode.REMOTE_REINDEX.getConfigValue());
    row.put(ClusterMigrationState.PARTITION_COUNT, 1L);
    row.put(
        ClusterMigrationState.partitionKey(0, ClusterMigrationCopyStep.PARTITION_SPEC),
        new CopyPartition(0, "src", "tgt", "src", null, 1, null).toJson(MAPPER));
    row.put(
        ClusterMigrationState.partitionKey(0, ClusterMigrationCopyStep.PARTITION_STATUS),
        ClusterMigrationCopyStep.PARTITION_PAUSED);
    row.put(
        ClusterMigrationState.partitionKey(0, ClusterMigrationCopyStep.PARTITION_CURSOR),
        "node:4711");
    row.persist(DataHubUpgradeState.IN_PROGRESS);
    ClusterMigrationCopyStep step =
        new ClusterMigrationCopyStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, true, 0),
            CONVENTION,
            graph,
            "v1");

    // The mocked clusters stop the step after the cursors have been dealt with.
    step.executable().apply(context);

    ClusterMigrationRow after = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    assertNull(
        after.get(ClusterMigrationState.partitionKey(0, ClusterMigrationCopyStep.PARTITION_CURSOR)),
        "the task id must not be handed to the in-process copier as a scroll id");
    assertNull(
        after.get(ClusterMigrationState.partitionKey(0, ClusterMigrationCopyStep.PARTITION_STATUS)),
        "a pause under the other copier is forgotten too, so the partition starts over");
    assertEquals(after.get(ClusterMigrationState.COPY_MODE), CopyMode.IN_PROCESS.getConfigValue());
  }

  @Test
  public void testAPartitionRetriesATransientFailureFromTheStart() throws Exception {
    List<CrossClusterCopier.Checkpoint> resumedFrom = new ArrayList<>();
    CrossClusterCopier flaky =
        new CrossClusterCopier() {
          @Override
          public Result copy(
              CopyPartition partition,
              CrossClusterCopier.Checkpoint resumeFrom,
              Checkpointer checkpointer,
              BooleanSupplier shouldStop)
              throws IOException {
            resumedFrom.add(resumeFrom);
            if (resumedFrom.size() == 1) {
              // Three more pages landed, then the connection to the target timed out.
              checkpointer.checkpoint(new CrossClusterCopier.Checkpoint("page-5", 50, 50, 0, 0));
              throw new IOException("Timeout connecting to the target");
            }
            return new Result(50, 50, 30, 20, 0, false);
          }

          @Override
          public String name() {
            return "flaky";
          }
        };
    CrossClusterCopier.Checkpoint paused =
        new CrossClusterCopier.Checkpoint("page-2", 20, 20, 0, 0);

    CrossClusterCopier.Result result =
        ClusterMigrationCopyStep.copyWithRetries(
            new CopyPartition(0, "src", "tgt", "src", null, 50, null),
            paused,
            flaky,
            checkpoint -> {},
            () -> false,
            0L);

    assertTrue(result.isComplete());
    assertEquals(
        resumedFrom,
        Arrays.asList(paused, null),
        "the first attempt resumed the pause; the retry started the partition over, since a cursor"
            + " checkpointed before a failure may be past a page the failure lost");
  }

  @Test
  public void testMirrorOnlyMigrationCopiesNothing() throws Exception {
    ClusterMigrationCopyStep step =
        new ClusterMigrationCopyStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, false, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.SUCCEEDED);
    verify(sourceClient, never()).performLowLevelRequest(any(), any());
    assertFalse(
        ClusterMigrationRow.load(
                opContext,
                entityService,
                ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary"))
            .existed());
  }

  @Test
  public void testTheUsageSweepAfterAFlipWaitsForTheLiveWriters() throws Exception {
    // Usage was copied from "secondary" to "primary" and verified, and the operator flipped:
    // "primary" serves and mirrors back to "secondary". The step runs for that reverse mirror, and
    // the ended migration owes a sweep of the old write index on "secondary".
    ClusterMigrationRow ended =
        ClusterMigrationRow.load(
            opContext,
            entityService,
            ClusterMigrationState.upgradeId(SearchComponent.USAGE, "secondary", "primary"));
    ended.setStatus(ClusterMigrationState.Status.VERIFIED);
    ended.put(ClusterMigrationState.USAGE_POST_FLIP_SWEEP, "pending");
    ended.put(ClusterMigrationState.USAGE_LIVE_STAGING_INDEX, "datahub_usage_migration-live");
    ended.put(ClusterMigrationState.USAGE_LAST_SOURCE_WRITE_INDEX, "datahub_usage_event-000003");
    ended.persist(DataHubUpgradeState.SUCCEEDED);
    ClusterMigrationRow reverse =
        ClusterMigrationRow.load(
            opContext,
            entityService,
            ClusterMigrationState.upgradeId(SearchComponent.USAGE, "primary", "secondary"));
    reverse.setStatus(ClusterMigrationState.Status.PREPARED);
    reverse.persist(DataHubUpgradeState.IN_PROGRESS);
    ClusterMigrationCopyStep step =
        new ClusterMigrationCopyStep(
            opContext,
            entityService,
            registry(SearchComponent.USAGE, false, 0),
            CONVENTION,
            usage,
            "v1");

    // No pod has reported the reverse mirror live yet: an old pod may still write to the old
    // cluster's alias, so the sweep must not read it.
    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.SUCCEEDED);

    verify(sourceClient, never()).performLowLevelRequest(any(), any());
    verify(targetClient, never()).performLowLevelRequest(any(), any());
    assertEquals(
        ClusterMigrationRow.load(opContext, entityService, ended.getUrn().getId())
            .get(ClusterMigrationState.USAGE_POST_FLIP_SWEEP),
        "pending");
  }

  @Test
  public void testPrepareRefusesANewTargetWhileAnotherMigrationIsInFlight() throws Exception {
    // A migration to "other" is still copying; the configuration now points graph at "secondary".
    String inFlight = ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "other");
    pointGraphAt(inFlight, ClusterMigrationState.Status.COPYING);
    ClusterMigrationPrepareStep step =
        new ClusterMigrationPrepareStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, true, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.FAILED);
    verify(targetClient, never()).performLowLevelRequest(any(), any());
    assertEquals(activeGraphMigration(), inFlight);
  }

  @Test
  public void testAPrepareUnderANewRolloutStampsTheMarkersOut() throws Exception {
    // A deploy is a rollout: the row's markers came from the pods of the system update "v0"
    // rolled out, which say nothing about the pods "v1" is about to roll out. They are not
    // deleted, only dated: the stamp says which ones count.
    String upgradeId =
        ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary");
    ClusterMigrationRow row = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    row.setStatus(ClusterMigrationState.Status.PREPARED);
    row.put(ClusterMigrationState.PREPARED_BY, "v0");
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS), 5L);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.MAE), 7L);
    row.persist(DataHubUpgradeState.IN_PROGRESS);
    ClusterMigrationPrepareStep step =
        new ClusterMigrationPrepareStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, false, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.SUCCEEDED);

    ClusterMigrationRow after = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    assertTrue(
        ClusterMigrationState.liveWritersMirroringSince(after.state()).isEmpty(),
        "this rollout's pods report again before anything waits on them");
    assertEquals(after.get(ClusterMigrationState.PREPARED_BY), "v1");
    assertTrue(after.getLong(ClusterMigrationState.ROLLOUT_STARTED_AT).orElseThrow() > 7L);
    assertEquals(
        after.get(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS)),
        "5",
        "the old marker stays on the row; it is the stamp that retires it");
  }

  @Test
  public void testAVerifiedRowWhoseIndicesAreGoneIsPlannedAgainUnderItsRollout() throws Exception {
    // The same system update runs again after the target indices were deleted (a repair in the
    // dev stack): the plan starts over, but the pods are the same ones, so their markers and the
    // stamp that dates them stay, and the copy is not made to wait for a rollout that never comes.
    StubCluster target = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    target.on("HEAD", "/graph_service_v1", new StubCluster.Reply(404, ""));
    target.on("PUT", "/cluster_migration_log_graph", StubCluster.Reply.ok("{}"));
    targetClient = target.shim;
    String upgradeId =
        ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary");
    ClusterMigrationRow row = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    row.setStatus(ClusterMigrationState.Status.VERIFIED);
    row.put(ClusterMigrationState.PREPARED_BY, "v1");
    row.put(ClusterMigrationState.ROLLOUT_STARTED_AT, 3L);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS), 5L);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.MAE), 7L);
    row.put(ClusterMigrationState.PARTITION_COUNT, 4L);
    row.persist(DataHubUpgradeState.SUCCEEDED);
    ClusterMigrationPrepareStep step =
        new ClusterMigrationPrepareStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, true, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.SUCCEEDED);

    ClusterMigrationRow after = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    assertEquals(after.status().orElseThrow(), ClusterMigrationState.Status.PREPARED);
    assertNull(after.get(ClusterMigrationState.PARTITION_COUNT), "the plan starts over");
    assertEquals(after.get(ClusterMigrationState.PREPARED_BY), "v1");
    assertEquals(after.get(ClusterMigrationState.ROLLOUT_STARTED_AT), "3");
    assertEquals(after.get(ClusterMigrationState.COPY_ENABLED), "true");
    assertTrue(ClusterMigrationState.liveWritersMirroringSince(after.state()).isPresent());
  }

  @Test
  public void testAVerifiedRowWhoseIndicesAreGoneIsStampedByANewRollout() throws Exception {
    // A new deploy finds the target indices gone: the plan starts over, the old rollout's markers
    // come back with the merge, and the fresh stamp is what keeps them from counting.
    StubCluster target = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    target.on("HEAD", "/graph_service_v1", new StubCluster.Reply(404, ""));
    target.on("PUT", "/cluster_migration_log_graph", StubCluster.Reply.ok("{}"));
    targetClient = target.shim;
    String upgradeId =
        ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary");
    ClusterMigrationRow row = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    row.setStatus(ClusterMigrationState.Status.VERIFIED);
    row.put(ClusterMigrationState.PREPARED_BY, "v0");
    row.put(ClusterMigrationState.ROLLOUT_STARTED_AT, 3L);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS), 5L);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.MAE), 7L);
    row.persist(DataHubUpgradeState.SUCCEEDED);
    ClusterMigrationPrepareStep step =
        new ClusterMigrationPrepareStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, true, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.SUCCEEDED);

    ClusterMigrationRow after = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    assertEquals(after.status().orElseThrow(), ClusterMigrationState.Status.PREPARED);
    assertEquals(after.get(ClusterMigrationState.PREPARED_BY), "v1");
    assertTrue(after.getLong(ClusterMigrationState.ROLLOUT_STARTED_AT).orElseThrow() > 7L);
    assertEquals(
        after.get(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS)),
        "5",
        "the old markers stay on the row");
    assertTrue(
        ClusterMigrationState.liveWritersMirroringSince(after.state()).isEmpty(),
        "and do not count: this rollout's pods must report");
  }

  @Test
  public void testAWindowedPassSentBackIsRunInFullAgain() {
    ClusterMigrationRow row =
        ClusterMigrationRow.load(
            opContext,
            entityService,
            ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary"));
    row.put(
        ClusterMigrationState.partitionKey(0, ClusterMigrationCopyStep.PARTITION_STATUS),
        ClusterMigrationCopyStep.PARTITION_DONE);
    row.put(
        ClusterMigrationState.partitionKey(1_000_000, ClusterMigrationCopyStep.PARTITION_STATUS),
        ClusterMigrationCopyStep.PARTITION_DONE);
    row.put(
        ClusterMigrationState.partitionKey(1_000_000, ClusterMigrationCopyStep.PARTITION_READ), 9L);

    ClusterMigrationCopyStep.forgetWindowPartitions(row);

    assertEquals(
        row.get(ClusterMigrationState.partitionKey(0, ClusterMigrationCopyStep.PARTITION_STATUS)),
        ClusterMigrationCopyStep.PARTITION_DONE,
        "the copy's own partitions keep their state");
    assertNull(
        row.get(
            ClusterMigrationState.partitionKey(
                1_000_000, ClusterMigrationCopyStep.PARTITION_STATUS)));
    assertNull(
        row.get(
            ClusterMigrationState.partitionKey(
                1_000_000, ClusterMigrationCopyStep.PARTITION_READ)));
  }

  @Test
  public void testAMarkerRecordedWhileAStepWorksSurvivesItsPersist() throws Exception {
    String upgradeId =
        ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary");
    ClusterMigrationRow stepsView = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    stepsView.setStatus(ClusterMigrationState.Status.PREPARED);
    stepsView.persist(DataHubUpgradeState.IN_PROGRESS);
    // A pod's poll records its marker between the step's load and its persist.
    ClusterMigrationRow podsView = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    podsView.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS), 9L);
    podsView.persist(DataHubUpgradeState.IN_PROGRESS);

    stepsView.put(ClusterMigrationState.COPY_MODE, "inProcess");
    stepsView.persist(DataHubUpgradeState.IN_PROGRESS);

    ClusterMigrationRow after = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    assertEquals(
        after.get(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS)),
        "9");
    assertEquals(after.get(ClusterMigrationState.COPY_MODE), "inProcess");
  }

  @Test
  public void testAPrepareUnderTheSameRolloutKeepsTheMarkers() throws Exception {
    // The system update of one deploy run again (the dev stack's copy command, a restarted job):
    // the pods have not changed, so what they reported still holds.
    String upgradeId =
        ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary");
    ClusterMigrationRow row = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    row.setStatus(ClusterMigrationState.Status.PREPARED);
    row.put(ClusterMigrationState.PREPARED_BY, "v1");
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS), 5L);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.MAE), 7L);
    row.persist(DataHubUpgradeState.IN_PROGRESS);
    ClusterMigrationPrepareStep step =
        new ClusterMigrationPrepareStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, false, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.SUCCEEDED);

    ClusterMigrationRow after = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    assertTrue(ClusterMigrationState.liveWritersMirroringSince(after.state()).isPresent());
  }

  @Test
  public void testAPrepareSetsACopyLeftOnAsideOnceTheReverseMigrationIsVerified() throws Exception {
    // The flip with COPY_ENABLED left at its default: "secondary" holds everything "primary" had
    // and mirrors back, so there is nothing to copy and a copy would only block the rollback.
    ClusterMigrationRow verified =
        ClusterMigrationRow.load(
            opContext,
            entityService,
            ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "secondary", "primary"));
    verified.setStatus(ClusterMigrationState.Status.VERIFIED);
    verified.persist(DataHubUpgradeState.SUCCEEDED);
    String upgradeId =
        ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary");
    ClusterMigrationPrepareStep step =
        new ClusterMigrationPrepareStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, true, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.SUCCEEDED);

    ClusterMigrationRow row = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    assertEquals(row.get(ClusterMigrationState.COPY_ENABLED), "false");
    assertEquals(row.status().orElseThrow(), ClusterMigrationState.Status.PREPARED);
    // No delete log was created on the target: the pair mirrors only.
    verify(targetClient, never()).performLowLevelRequest(any(), any());
  }

  @Test
  public void testTheCopyStepFollowsThePrepareStepsCopyDecision() throws Exception {
    String upgradeId =
        ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary");
    ClusterMigrationRow row = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    row.setStatus(ClusterMigrationState.Status.PREPARED);
    row.put(ClusterMigrationState.COPY_ENABLED, "false");
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS), 5L);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.MAE), 7L);
    row.persist(DataHubUpgradeState.IN_PROGRESS);
    ClusterMigrationCopyStep step =
        new ClusterMigrationCopyStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, true, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.SUCCEEDED);

    verify(sourceClient, never()).performLowLevelRequest(any(), any());
    assertEquals(
        ClusterMigrationRow.load(opContext, entityService, upgradeId).status().orElseThrow(),
        ClusterMigrationState.Status.PREPARED);
  }

  @Test
  public void testAPrepareOfAVerifiedRowForgetsTheMarkersEvenWithTheCopyLeftOn() throws Exception {
    // A rollback that left COPY_ENABLED at its default: the forward row is verified and will never
    // copy again, but the usage sweep must still wait for this rollout's pods.
    StubCluster target = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    target.on("HEAD", "/graph_service_v1", StubCluster.Reply.ok(""));
    targetClient = target.shim;
    String upgradeId =
        ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary");
    ClusterMigrationRow row = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    row.setStatus(ClusterMigrationState.Status.VERIFIED);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.GMS), 5L);
    row.put(ClusterMigrationState.mirrorActiveSinceKey(ClusterMigrationState.Process.MAE), 7L);
    row.persist(DataHubUpgradeState.SUCCEEDED);
    ClusterMigrationPrepareStep step =
        new ClusterMigrationPrepareStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, true, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.SUCCEEDED);

    ClusterMigrationRow after = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    assertEquals(after.status().orElseThrow(), ClusterMigrationState.Status.VERIFIED);
    assertTrue(ClusterMigrationState.liveWritersMirroringSince(after.state()).isEmpty());
  }

  @Test
  public void testAPauseIsResumedOnceAndThenForgotten() throws Exception {
    String upgradeId =
        ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary");
    ClusterMigrationRow row = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    row.put(
        ClusterMigrationState.partitionKey(0, ClusterMigrationCopyStep.PARTITION_STATUS),
        ClusterMigrationCopyStep.PARTITION_PAUSED);
    row.put(ClusterMigrationState.partitionKey(0, ClusterMigrationCopyStep.PARTITION_CURSOR), "S");
    row.put(ClusterMigrationState.partitionKey(0, ClusterMigrationCopyStep.PARTITION_READ), 20L);
    row.put(ClusterMigrationState.partitionKey(0, ClusterMigrationCopyStep.PARTITION_CREATED), 15L);
    row.put(ClusterMigrationState.partitionKey(0, ClusterMigrationCopyStep.PARTITION_PRESENT), 5L);
    row.put(ClusterMigrationState.partitionKey(0, ClusterMigrationCopyStep.PARTITION_FAILURES), 0L);
    row.persist(DataHubUpgradeState.IN_PROGRESS);
    CopyPartition partition = new CopyPartition(0, "src", "tgt", "src", null, 50, null);

    CrossClusterCopier.Checkpoint resumeFrom =
        ClusterMigrationCopyStep.claimPartition(row, partition);

    assertEquals(resumeFrom, new CrossClusterCopier.Checkpoint("S", 20, 15, 5, 0));
    // The row no longer says paused: a cursor this attempt persists on its way is not resumed from
    // by the next run should this one die, since the scroll may already be past a lost page.
    ClusterMigrationRow persisted = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    assertNull(
        persisted.get(
            ClusterMigrationState.partitionKey(0, ClusterMigrationCopyStep.PARTITION_STATUS)));
    assertNull(
        persisted.get(
            ClusterMigrationState.partitionKey(0, ClusterMigrationCopyStep.PARTITION_CURSOR)));
    assertNull(ClusterMigrationCopyStep.claimPartition(row, partition), "a retry starts over");
  }

  @Test
  public void testPrepareMovesThePointerPastAMigrationThatOnlyPrepared() throws Exception {
    // The rollback after a flip: the reverse mirror's row is prepared and will never go further,
    // and the configuration points graph back at "secondary" with the copy off.
    String reverseMirror =
        ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "secondary", "primary");
    pointGraphAt(reverseMirror, ClusterMigrationState.Status.PREPARED);
    ClusterMigrationPrepareStep step =
        new ClusterMigrationPrepareStep(
            opContext,
            entityService,
            registry(SearchComponent.GRAPH, false, 0),
            CONVENTION,
            graph,
            "v1");

    assertEquals(step.executable().apply(context).result(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(
        activeGraphMigration(),
        ClusterMigrationState.upgradeId(SearchComponent.GRAPH, "primary", "secondary"));
    assertEquals(
        ClusterMigrationRow.load(opContext, entityService, reverseMirror).status().orElseThrow(),
        ClusterMigrationState.Status.PREPARED,
        "the row left behind is untouched");
  }

  private void pointGraphAt(String upgradeId, ClusterMigrationState.Status status) {
    ClusterMigrationRow pointer =
        ClusterMigrationRow.load(
            opContext, entityService, ClusterMigrationPrepareStep.pointerId(SearchComponent.GRAPH));
    pointer.put(ClusterMigrationPrepareStep.POINTER_ACTIVE_UPGRADE_ID, upgradeId);
    pointer.persist(DataHubUpgradeState.SUCCEEDED);
    ClusterMigrationRow active = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    active.setStatus(status);
    active.persist(DataHubUpgradeState.IN_PROGRESS);
  }

  private String activeGraphMigration() {
    return ClusterMigrationRow.load(
            opContext, entityService, ClusterMigrationPrepareStep.pointerId(SearchComponent.GRAPH))
        .get(ClusterMigrationPrepareStep.POINTER_ACTIVE_UPGRADE_ID);
  }
}
