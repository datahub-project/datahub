package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_STRUCT_PROPS_DISABLED;
import static io.datahubproject.test.search.SearchTestUtils.TEST_GRAPH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SYSTEM_METADATA_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_TIMESERIES_ASPECT_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeContext;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeReport;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.UsageEventIndexSetup;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.search.ClusterMigrationConfiguration;
import com.linkedin.metadata.config.search.ComponentClusterConfiguration;
import com.linkedin.metadata.config.search.CopySettings;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.FamilyMigrationSettings;
import com.linkedin.metadata.config.search.RefreshIntervals;
import com.linkedin.metadata.config.search.RemoteCopySettings;
import com.linkedin.metadata.config.search.SearchClusterIndexSettings;
import com.linkedin.metadata.config.search.SearchClusterSettings;
import com.linkedin.metadata.config.search.SearchClusterUri;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultStore;
import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO;
import com.linkedin.metadata.graph.elastic.ESGraphWriteDAO;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ClusterMigrationState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.UpgradeRowMirrorLiveMarker;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testng.SkipException;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

/**
 * Two search clusters, a source and a target, with the registry, mirrored processors and the
 * migration upgrades wired the way system-update wires them. Primary storage is an in-memory
 * upgrade-row store: the only aspect these steps touch. Each phase of the operator sequence is a
 * new harness over the same clusters and rows, like a redeploy.
 *
 * <p>Two ways to get clusters:
 *
 * <ul>
 *   <li><b>Containers</b> (default): an OpenSearch 2 source and an OpenSearch 3 target started
 *       through Testcontainers, under the fixed prefixes {@code acme} and {@code tenant}.
 *   <li><b>Remote</b>: set {@code CLUSTER_MIGRATION_TEST_SOURCE_URI} and {@code
 *       CLUSTER_MIGRATION_TEST_TARGET_URI} (plus {@code _USERNAME}/{@code _PASSWORD}, or {@code
 *       _USE_AWS_IAM_AUTH=true} with {@code _REGION}, per side) to run the same scenarios against
 *       real clusters, for example a staging OpenSearch 2 domain and the OpenSearch 3 domain. Every
 *       index, policy and template the run creates carries a unique per-run prefix ({@code
 *       CLUSTER_MIGRATION_TEST_PREFIX} or a random one) and is deleted when the owning harness
 *       closes, so a shared cluster is left as it was found. {@code
 *       CLUSTER_MIGRATION_TEST_COPY_MODE} selects the copier ({@code inProcess} default, or {@code
 *       remoteReindex} with {@code CLUSTER_MIGRATION_TEST_REMOTE_EXTERNAL=true} between AWS VPC
 *       domains and optional {@code _REMOTE_USERNAME}/{@code _REMOTE_PASSWORD} overrides).
 * </ul>
 *
 * <p>{@code CLUSTER_MIGRATION_TEST_SCALE} multiplies every seeded document count (default 1, a few
 * hundred documents per index). A scaled run also switches the copy settings from the tiny pages
 * and partitions that exercise every code path to production-sized ones, so the copy timing the
 * test prints is about the copier; {@code CLUSTER_MIGRATION_TEST_COPY_PARALLELISM} and {@code
 * CLUSTER_MIGRATION_TEST_COPY_BATCH_SIZE} override those two settings for comparisons.
 */
final class ClusterMigrationHarness implements AutoCloseable {
  static final String OPT_IN = SearchV3MigrationHarness.OPT_IN;
  static final String ENV_PREFIX = "CLUSTER_MIGRATION_TEST_";
  private static final String CONTAINER_SOURCE_PREFIX = "acme";
  private static final String CONTAINER_TARGET_PREFIX = "tenant";
  static final String SOURCE_ALIAS = "source";
  private static final String OPENSEARCH_IMAGE = "opensearchproject/opensearch";
  private static final String SOURCE_VERSION = "2.19.3";
  private static final String TARGET_VERSION = "3.7.0";
  private static final int HTTP_PORT = 9200;
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration CLIENT_CONNECT_TIMEOUT = Duration.ofSeconds(60);
  private static final long CLIENT_CONNECT_RETRY_MILLIS = 500;

  enum Phase {
    /** Families serve from the source; writes mirror to the target; the copy runs. */
    MIGRATE(true, true),
    /** Families serve from the target; writes mirror back to the source; no copy. */
    FLIPPED(false, false),
    /** The rollback: families serve from the source again, mirrored to the target; no copy. */
    ROLLED_BACK(true, false);

    final boolean servesFromSource;
    final boolean copies;

    Phase(boolean servesFromSource, boolean copies) {
      this.servesFromSource = servesFromSource;
      this.copies = copies;
    }
  }

  /** One side of the migration as the harness reaches it. */
  record Endpoint(
      String uri,
      @Nullable String username,
      @Nullable String password,
      boolean awsIamAuth,
      @Nullable String region) {

    static Endpoint container(GenericContainer<?> container) {
      return new Endpoint(httpUri(container), null, null, false, null);
    }

    /**
     * The side's variables under {@code CLUSTER_MIGRATION_TEST_<SIDE>_*}, or null without a uri.
     */
    @Nullable
    static Endpoint fromEnv(String side) {
      String uri = env(side + "_URI");
      if (uri == null) {
        return null;
      }
      boolean iam = "true".equalsIgnoreCase(env(side + "_USE_AWS_IAM_AUTH"));
      String region = env(side + "_REGION");
      if (iam && region == null) {
        throw new IllegalArgumentException(
            ENV_PREFIX
                + side
                + "_REGION is required with "
                + ENV_PREFIX
                + side
                + "_USE_AWS_IAM_AUTH");
      }
      return new Endpoint(uri, env(side + "_USERNAME"), env(side + "_PASSWORD"), iam, region);
    }

    SearchClusterSettings settings(@Nullable String prefixOverlay) {
      SearchClusterSettings.SearchClusterSettingsBuilder builder =
          SearchClusterSettings.builder()
              .uri(uri)
              .username(username)
              .password(password)
              .opensearchUseAwsIamAuth(awsIamAuth)
              .region(region)
              .dataNodeCount(1);
      if (prefixOverlay != null) {
        builder.index(SearchClusterIndexSettings.builder().prefix(prefixOverlay).build());
      }
      return builder.build();
    }
  }

  @Nullable
  static String env(String suffix) {
    String value = System.getenv(ENV_PREFIX + suffix);
    return value == null || value.trim().isEmpty() ? null : value.trim();
  }

  static int envInt(String suffix, int fallback) {
    String value = env(suffix);
    return value == null ? fallback : Integer.parseInt(value);
  }

  /** True when both sides are configured in the environment: no containers are started then. */
  static boolean remoteConfigured() {
    return Endpoint.fromEnv("SOURCE") != null && Endpoint.fromEnv("TARGET") != null;
  }

  /** Multiplies every seeded document count; 1 is the small container run. */
  static int scale() {
    return envInt("SCALE", 1);
  }

  /**
   * The source as the target cluster's nodes reach it for a remote reindex: the docker network
   * alias between containers, or {@code CLUSTER_MIGRATION_TEST_REMOTE_HOST} (null means the source
   * uri itself) against real clusters.
   */
  @Nullable
  private static String remoteHost() {
    return remoteConfigured() ? env("REMOTE_HOST") : "http://" + SOURCE_ALIAS + ":" + HTTP_PORT;
  }

  final Phase phase;
  @Nullable final GenericContainer<?> sourceContainer;
  @Nullable final GenericContainer<?> targetContainer;
  final Endpoint source;
  final Endpoint target;
  final String sourcePrefix;
  final String targetPrefix;
  final boolean remote;
  final SearchClientShim<?> sourceClient;
  final SearchClientShim<?> targetClient;
  final ElasticSearchConfiguration config;
  final IndexConvention indexConvention;
  final EntityRegistry entityRegistry;
  final SearchClusterRegistry registry;
  final OperationContext opContext;
  final InMemoryUpgradeRows rows;
  final EntityService<?> entityService;
  final ElasticSearchGraphService graphService;
  final ElasticSearchSystemMetadataService systemMetadataService;
  final ElasticSearchTimeseriesAspectService timeseriesAspectService;
  final ClusterMigrationPrepare prepare;
  final ClusterMigrationCopy copy;
  @Nullable private final Network network;
  private final boolean ownsClusters;

  static void requireOptIn() {
    if (!"true".equalsIgnoreCase(System.getenv(OPT_IN))) {
      throw new SkipException(
          "Cluster migration tests are opt-in: set "
              + OPT_IN
              + "=true or run :datahub-upgrade:searchV3MigrationTest /"
              + " :datahub-upgrade:clusterMigrationRemoteTest");
    }
    if (!remoteConfigured() && !DockerClientFactory.instance().isDockerAvailable()) {
      throw new SkipException(
          "Docker is not available and no remote clusters are configured ("
              + ENV_PREFIX
              + "SOURCE_URI / "
              + ENV_PREFIX
              + "TARGET_URI)");
    }
  }

  static ClusterMigrationHarness start() throws Exception {
    if (remoteConfigured()) {
      String base = env("PREFIX");
      if (base == null) {
        base = "mt" + Long.toHexString(System.nanoTime() & 0xffffffL);
      }
      if (!base.matches("[a-z][a-z0-9]{1,14}")) {
        throw new IllegalArgumentException(
            ENV_PREFIX + "PREFIX must be lower-case alphanumerics starting with a letter: " + base);
      }
      return new ClusterMigrationHarness(
          Phase.MIGRATE,
          null,
          null,
          null,
          Endpoint.fromEnv("SOURCE"),
          Endpoint.fromEnv("TARGET"),
          base + "a",
          base + "b",
          new InMemoryUpgradeRows(),
          true);
    }
    Network network = Network.newNetwork();
    GenericContainer<?> source =
        openSearch(System.getenv("OPENSEARCH_IMAGE_FULL_NAME"), SOURCE_VERSION)
            .withNetwork(network)
            .withNetworkAliases(SOURCE_ALIAS);
    GenericContainer<?> target =
        openSearch(System.getenv("OPENSEARCH_TARGET_IMAGE_FULL_NAME"), TARGET_VERSION)
            .withNetwork(network)
            .withEnv("reindex.remote.allowlist", SOURCE_ALIAS + ":" + HTTP_PORT);
    Startables.deepStart(List.of(source, target)).join();
    return new ClusterMigrationHarness(
        Phase.MIGRATE,
        source,
        target,
        network,
        Endpoint.container(source),
        Endpoint.container(target),
        CONTAINER_SOURCE_PREFIX,
        CONTAINER_TARGET_PREFIX,
        new InMemoryUpgradeRows(),
        true);
  }

  /** The next deploy: every family served from the target, mirrored back to the source. */
  ClusterMigrationHarness redeployFlipped() throws Exception {
    return redeploy(Phase.FLIPPED);
  }

  /** The rollback: every family served from the source again, mirrored to the target, no copy. */
  ClusterMigrationHarness redeployRolledBack() throws Exception {
    return redeploy(Phase.ROLLED_BACK);
  }

  private ClusterMigrationHarness redeploy(Phase phase) throws Exception {
    return new ClusterMigrationHarness(
        phase,
        sourceContainer,
        targetContainer,
        network,
        source,
        target,
        sourcePrefix,
        targetPrefix,
        rows,
        false);
  }

  private ClusterMigrationHarness(
      Phase phase,
      @Nullable GenericContainer<?> sourceContainer,
      @Nullable GenericContainer<?> targetContainer,
      @Nullable Network network,
      Endpoint source,
      Endpoint target,
      String sourcePrefix,
      String targetPrefix,
      InMemoryUpgradeRows rows,
      boolean ownsClusters)
      throws Exception {
    this.phase = phase;
    this.sourceContainer = sourceContainer;
    this.targetContainer = targetContainer;
    this.network = network;
    this.source = source;
    this.target = target;
    this.sourcePrefix = sourcePrefix;
    this.targetPrefix = targetPrefix;
    this.remote = sourceContainer == null;
    this.rows = rows;
    this.ownsClusters = ownsClusters;
    this.entityService = rows.entityService();

    // A client's connection pool follows its thread count and bounds the copy's parallelism.
    sourceClient = client(source, copyParallelism());
    targetClient = client(target, copyParallelism());
    config = configuration(phase, source, target, targetPrefix);
    entityRegistry = TestOperationContexts.defaultEntityRegistry();
    indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver(sourcePrefix),
            config.getEntityIndex(),
            IndexConventionImpl.explicitComponentPrefixOverlays(config));

    Map<String, SearchClusterRegistry.ClusterConnection> connections = new LinkedHashMap<>();
    connections.put(
        "primary",
        new SearchClusterRegistry.ClusterConnection(
            "primary",
            config,
            sourceClient,
            bulkProcessor(sourceClient),
            indexBuilder(sourceClient)));
    connections.put(
        "secondary",
        new SearchClusterRegistry.ClusterConnection(
            "secondary",
            config,
            targetClient,
            bulkProcessor(targetClient),
            indexBuilder(targetClient)));
    registry = new SearchClusterRegistry(config, connections, indexConvention, null);

    OperationContext base =
        TestOperationContexts.systemContextNoSearchAuthorization(
            entityRegistry,
            SearchContext.builder()
                .indexConvention(indexConvention)
                .searchClusterAccess(registry)
                .build());
    opContext = base;

    SearchClientShim<?> servingClient = phase.servesFromSource ? sourceClient : targetClient;
    graphService =
        new ElasticSearchGraphService(
            new LineageRegistry(entityRegistry),
            registry.bulkProcessorFor(SearchComponent.GRAPH),
            indexConvention,
            new ESGraphWriteDAO(
                indexConvention,
                registry.bulkProcessorFor(SearchComponent.GRAPH),
                1,
                config.getSearch().getGraph()),
            new ESGraphQueryDAO(servingClient, TEST_GRAPH_SERVICE_CONFIG, config, null),
            registry.indexBuilderFor(SearchComponent.GRAPH),
            "MD5");
    systemMetadataService =
        new ElasticSearchSystemMetadataService(
            registry.bulkProcessorFor(SearchComponent.SYSTEM_METADATA),
            indexConvention,
            new ESSystemMetadataDAO(
                servingClient,
                indexConvention,
                registry.bulkProcessorFor(SearchComponent.SYSTEM_METADATA),
                1,
                TEST_SYSTEM_METADATA_SERVICE_CONFIG),
            registry.indexBuilderFor(SearchComponent.SYSTEM_METADATA),
            "MD5",
            TEST_SYSTEM_METADATA_SERVICE_CONFIG);
    timeseriesAspectService =
        new ElasticSearchTimeseriesAspectService(
            servingClient,
            registry.bulkProcessorFor(SearchComponent.TIMESERIES),
            1,
            QueryFilterRewriteChain.EMPTY,
            TEST_TIMESERIES_ASPECT_SERVICE_CONFIG,
            entityRegistry,
            indexConvention,
            registry.indexBuilderFor(SearchComponent.TIMESERIES),
            null);

    String upgradeVersion = "0.0.0-test-" + phase.name().toLowerCase();
    prepare =
        new ClusterMigrationPrepare(
            opContext,
            entityService,
            registry,
            indexConvention,
            graphService,
            systemMetadataService,
            timeseriesAspectService,
            upgradeVersion);
    copy =
        new ClusterMigrationCopy(
            opContext,
            entityService,
            registry,
            indexConvention,
            graphService,
            systemMetadataService,
            timeseriesAspectService,
            upgradeVersion);
  }

  // ---- running the upgrades -----------------------------------------------------------------

  UpgradeContext context(Upgrade upgrade) {
    return new DefaultUpgradeContext(
        opContext, upgrade, new DefaultUpgradeReport(), List.of(), List.of());
  }

  /** Wall-clock time of each step the last {@link #run} executed, by step id. */
  final Map<String, Duration> stepDurations = new LinkedHashMap<>();

  /** Runs every step of the upgrade that does not skip itself and returns their results by id. */
  Map<String, DataHubUpgradeState> run(Upgrade upgrade) {
    UpgradeContext context = context(upgrade);
    Map<String, DataHubUpgradeState> results = new LinkedHashMap<>();
    stepDurations.clear();
    for (UpgradeStep step : upgrade.steps()) {
      if (step.skip(context)) {
        continue;
      }
      Instant started = Instant.now();
      UpgradeStepResult result = step.executable().apply(context);
      stepDurations.put(step.id(), Duration.between(started, Instant.now()));
      results.put(step.id(), result.result());
    }
    return results;
  }

  /** Marks both live writers' mirrors live for a family, the way GMS's and MAE's polls would. */
  void markLiveWriters(SearchComponent family) throws Exception {
    SearchClusterRegistry.FamilyMigration migration = registry.migrationFor(family).orElseThrow();
    Urn urn =
        BootstrapStep.getUpgradeUrn(
            ClusterMigrationState.upgradeId(
                family, migration.getSourceCluster(), migration.getTargetCluster()));
    UpgradeRowMirrorLiveMarker marker =
        new UpgradeRowMirrorLiveMarker(
            rows,
            opContext,
            urn,
            EnumSet.of(ClusterMigrationState.Process.GMS, ClusterMigrationState.Process.MAE),
            migration.getWriteLog());
    marker.poll();
    if (!marker.isRecorded()) {
      throw new IllegalStateException("live-writer markers not recorded for " + urn);
    }
  }

  ClusterMigrationRow row(SearchComponent family, String source, String target) {
    return ClusterMigrationRow.load(
        opContext, entityService, ClusterMigrationState.upgradeId(family, source, target));
  }

  // ---- cluster access -----------------------------------------------------------------------

  ClusterHttp source() {
    return new ClusterHttp(opContext, sourceClient);
  }

  ClusterHttp target() {
    return new ClusterHttp(opContext, targetClient);
  }

  BaseElasticSearchComponentsFactory.BaseElasticSearchComponents sourceComponents() {
    return registry.connection("primary").asComponents(indexConvention);
  }

  /**
   * Flushes every cluster's processor, waits for the bulks to complete and refreshes both clusters,
   * so a read right after sees everything written. A bulk item failure surfaces as the processor's
   * transfer exception.
   */
  void flushAll() throws Exception {
    for (ESBulkProcessor processor : registry.bulkProcessors()) {
      processor.flushAndWait(Duration.ofSeconds(60));
    }
    source().send("POST", "/_refresh", null);
    target().send("POST", "/_refresh", null);
  }

  /** Creates the family indices and the usage layout on the source the way Phase 1 would. */
  void createServingIndices() throws Exception {
    ESIndexBuilder sourceBuilder = registry.connection("primary").getIndexBuilder();
    for (ReindexConfig config : graphService.buildReindexConfigs(opContext, List.of())) {
      sourceBuilder.buildIndex(opContext, config);
    }
    for (ReindexConfig config : systemMetadataService.buildReindexConfigs(opContext, List.of())) {
      sourceBuilder.buildIndex(opContext, config);
    }
    for (ReindexConfig config : timeseriesAspectService.buildReindexConfigs(opContext, List.of())) {
      sourceBuilder.buildIndex(opContext, config);
    }
    UsageEventIndexSetup.createFresh(
        opContext, sourceComponents(), UsageEventIndexSetup.Names.of(sourcePrefix + "_"), 1, 0, 1);
  }

  /** A document to seed: its id and JSON source. */
  record Doc(String id, String source) {}

  // Small managed domains cap a request body at 10 MB; keep well under it with large documents.
  private static final int SEED_DOCS_PER_REQUEST = 2000;
  private static final int SEED_CHARS_PER_REQUEST = 4_000_000;

  /**
   * Bulk-creates documents directly on a cluster, bypassing every mirror, a few thousand per
   * request, and refreshes the index once at the end.
   */
  static void bulkCreate(ClusterHttp cluster, String index, Stream<Doc> docs) throws IOException {
    StringBuilder ndjson = new StringBuilder();
    int pending = 0;
    for (Iterator<Doc> it = docs.iterator(); it.hasNext(); ) {
      Doc doc = it.next();
      ndjson
          .append("{\"create\":{\"_index\":\"")
          .append(index)
          .append("\",\"_id\":\"")
          .append(doc.id())
          .append("\"}}\n")
          .append(doc.source())
          .append('\n');
      if (++pending == SEED_DOCS_PER_REQUEST || ndjson.length() >= SEED_CHARS_PER_REQUEST) {
        sendBulk(cluster, index, ndjson);
        ndjson.setLength(0);
        pending = 0;
      }
    }
    if (pending > 0) {
      sendBulk(cluster, index, ndjson);
    }
    cluster.refresh(index);
  }

  private static void sendBulk(ClusterHttp cluster, String index, StringBuilder ndjson)
      throws IOException {
    JsonNode response = cluster.raw("POST", "/_bulk", ndjson.toString(), "application/x-ndjson");
    if (!response.path("errors").asBoolean()) {
      return;
    }
    for (JsonNode item : response.path("items")) {
      JsonNode error = item.path("create").path("error");
      if (!error.isMissingNode()) {
        throw new IOException("Seeding " + index + " failed: " + error);
      }
    }
    throw new IOException("Seeding " + index + " failed: " + response);
  }

  String servingName(SearchComponent family, String baseName) {
    return indexConvention.getIndexName(opContext, family, baseName);
  }

  /** The migration the family's pointer row names. */
  String activeMigration(SearchComponent family) {
    return ClusterMigrationRow.load(
            opContext, entityService, ClusterMigrationPrepareStep.pointerId(family))
        .get(ClusterMigrationPrepareStep.POINTER_ACTIVE_UPGRADE_ID);
  }

  String timeseriesIndex(String entity, String aspect) {
    return indexConvention.getTimeseriesAspectIndexName(opContext, entity, aspect);
  }

  /** The target cluster's name for an index the source serves under {@code servingName}. */
  String targetName(String servingName) {
    if (!servingName.startsWith(sourcePrefix + "_")) {
      throw new IllegalArgumentException(servingName + " is not under prefix " + sourcePrefix);
    }
    return targetPrefix + servingName.substring(sourcePrefix.length());
  }

  /** Each harness owns its clients and processors; the clusters belong to the first one. */
  @Override
  public void close() throws Exception {
    for (ESBulkProcessor processor : registry.bulkProcessors()) {
      processor.close();
    }
    if (ownsClusters) {
      if (remote) {
        cleanupRemote(source(), sourcePrefix);
        cleanupRemote(target(), targetPrefix);
      } else {
        targetContainer.stop();
        sourceContainer.stop();
        network.close();
      }
    }
    sourceClient.close();
    targetClient.close();
  }

  /**
   * Removes everything a run created under {@code prefix} on a shared cluster: the usage policy and
   * template, the usage data stream, then every index. Indices are listed and deleted one by one
   * because managed clusters may refuse wildcard deletes.
   */
  static void cleanupRemote(ClusterHttp cluster, String prefix) {
    try {
      UsageEventIndexSetup.Names usage = UsageEventIndexSetup.Names.of(prefix + "_");
      String[] absent = {"400", "404", "405"};
      cluster.send("DELETE", "/_data_stream/" + usage.writeTarget(), null, absent);
      for (String index : cluster.resolveIndices(prefix + "_*")) {
        cluster.deleteIndex(index);
      }
      cluster.send("DELETE", "/_index_template/" + usage.template(), null, absent);
      cluster.send("DELETE", "/_plugins/_ism/policies/" + usage.policy(), null, absent);
      cluster.send("DELETE", "/_ilm/policy/" + usage.policy(), null, absent);
    } catch (Exception e) {
      System.err.println("Cleanup of " + prefix + "_* left something behind: " + e);
    }
  }

  // ---- wiring helpers -----------------------------------------------------------------------

  private static ElasticSearchConfiguration configuration(
      Phase phase, Endpoint source, Endpoint target, String targetPrefix) {
    Map<String, SearchClusterSettings> clusters = new LinkedHashMap<>();
    clusters.put("primary", source.settings(null));
    clusters.put("secondary", target.settings(targetPrefix));
    String serving = phase.servesFromSource ? "primary" : "secondary";
    String targetName = phase.servesFromSource ? "secondary" : "primary";
    String copyMode = env("COPY_MODE") == null ? "inProcess" : env("COPY_MODE");
    int scale = scale();
    // The unscaled run uses tiny pages and partitions so every code path (splitting, paging,
    // checkpoints) runs on a few hundred documents; a scaled run uses production-sized pages so the
    // timing it prints reflects the copier rather than the number of round trips.
    int parallelism = copyParallelism();
    int batchSize = envInt("COPY_BATCH_SIZE", Math.min(1000, 25 * scale));
    FamilyMigrationSettings family =
        FamilyMigrationSettings.builder()
            .targetCluster(targetName)
            .copy(
                CopySettings.builder()
                    .enabled(phase.copies)
                    .mode(copyMode)
                    .parallelism(parallelism)
                    .batchSize(batchSize)
                    .requestsPerSecond(-1)
                    .targetDocsPerPartition(40 * scale)
                    .skewMs(0)
                    .cursorKeepAlive("2m")
                    .checkpointEveryPages(scale == 1 ? 1 : 10)
                    .remote(
                        RemoteCopySettings.builder()
                            .host(remoteHost())
                            .username(env("REMOTE_USERNAME"))
                            .password(env("REMOTE_PASSWORD"))
                            .external("true".equalsIgnoreCase(env("REMOTE_EXTERNAL")))
                            .socketTimeout("5m")
                            .connectTimeout("30s")
                            .build())
                    .build())
            .build();
    ElasticSearchConfiguration base = TEST_OS_SEARCH_CONFIG;
    return base.toBuilder()
        .clusters(clusters)
        .componentCluster(
            ComponentClusterConfiguration.builder()
                .graph(serving)
                .timeseries(serving)
                .systemMetadata(serving)
                .usage(serving)
                .build())
        .clusterMigration(
            ClusterMigrationConfiguration.builder()
                .processRole("upgrade")
                .graph(family)
                .timeseries(family)
                .systemMetadata(family)
                .usage(family)
                .build())
        .index(
            base.getIndex().toBuilder()
                .numShards(1)
                .numReplicas(0)
                .refreshIntervals(RefreshIntervals.allServices(1))
                .build())
        .build();
  }

  private static GenericContainer<?> openSearch(@Nullable String imageOverride, String version) {
    String image =
        imageOverride == null || imageOverride.isEmpty()
            ? OPENSEARCH_IMAGE + ":" + version
            : imageOverride;
    GenericContainer<?> container =
        new OpensearchContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor(OPENSEARCH_IMAGE));
    container
        .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
        .withEnv("DISABLE_SECURITY_PLUGIN", "true")
        // A developer machine near its Docker disk watermark would otherwise make the cluster
        // refuse the staging indices the copy creates mid-run.
        .withEnv("cluster.routing.allocation.disk.threshold_enabled", "false")
        .withStartupTimeout(STARTUP_TIMEOUT);
    return container;
  }

  private static String httpUri(GenericContainer<?> container) {
    return "http://" + container.getHost() + ":" + container.getMappedPort(HTTP_PORT);
  }

  private static int copyParallelism() {
    return envInt("COPY_PARALLELISM", scale() == 1 ? 2 : 4);
  }

  private static SearchClientShim<?> client(Endpoint endpoint, int threadCount) throws IOException {
    SearchClusterUri uri = SearchClusterUri.parse("test", endpoint.uri());
    SearchClientShimUtil.ShimConfigurationBuilder builder =
        new SearchClientShimUtil.ShimConfigurationBuilder()
            .withHost(uri.getHost())
            .withPort(uri.getPort())
            .withSSL(uri.isUseSSL())
            .withPathPrefix(uri.getPathPrefix())
            .withThreadCount(threadCount)
            .withConnectionRequestTimeout(30000)
            .withSocketTimeout(120000);
    if (endpoint.username() != null && endpoint.password() != null) {
      builder.withCredentials(endpoint.username(), endpoint.password());
    }
    if (endpoint.awsIamAuth()) {
      builder
          .withAwsIamAuth(true, endpoint.region())
          .withAwsCredentialsProvider(DefaultCredentialsProvider.builder().build());
    }
    SearchClientShim.ShimConfiguration configuration = builder.build();
    Instant deadline = Instant.now().plus(CLIENT_CONNECT_TIMEOUT);
    while (true) {
      try {
        return SearchClientShimUtil.createShimWithAutoDetection(configuration, new ObjectMapper());
      } catch (IOException e) {
        if (Instant.now().isAfter(deadline)) {
          throw e;
        }
        try {
          Thread.sleep(CLIENT_CONNECT_RETRY_MILLIS);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
  }

  private static ESBulkProcessor bulkProcessor(SearchClientShim<?> client) {
    return ESBulkProcessor.builder(client, null)
        .async(true)
        .writeRequestRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        .bulkRequestsLimit(100)
        .bulkFlushPeriod(1)
        .retryInterval(1L)
        .numRetries(1)
        .build();
  }

  private static ESIndexBuilder indexBuilder(SearchClientShim<?> client) {
    return new ESIndexBuilder(
        client,
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .index(TEST_OS_SEARCH_CONFIG.getIndex().toBuilder().numShards(1).numReplicas(0).build())
            .build(),
        TEST_ES_STRUCT_PROPS_DISABLED,
        Map.of(),
        new GitVersion("0.0.0-test", "123456", Optional.empty()));
  }

  /**
   * The {@code dataHubUpgradeResult} rows, in memory, served both as the store the live-writer
   * markers use and as the entity service the steps use. Versions advance on every write so the
   * conditional persist sees the same shape it sees against a real store.
   */
  static final class InMemoryUpgradeRows implements DataHubUpgradeResultStore {
    private final Map<Urn, DataHubUpgradeResult> rows = new ConcurrentHashMap<>();
    private final Map<Urn, Long> versions = new ConcurrentHashMap<>();

    @Override
    @Nullable
    public synchronized EnvelopedAspect readLatest(OperationContext opContext, @Nonnull Urn urn) {
      DataHubUpgradeResult result = rows.get(urn);
      if (result == null) {
        return null;
      }
      return DataHubUpgradeResultConditionalPersist.toEnveloped(
          result, String.valueOf(versions.getOrDefault(urn, 0L)));
    }

    @Override
    public synchronized void ingest(
        OperationContext opContext, @Nonnull MetadataChangeProposal proposal) {
      Urn urn = proposal.getEntityUrn();
      rows.put(
          urn,
          GenericRecordUtils.deserializeAspect(
              proposal.getAspect().getValue(),
              proposal.getAspect().getContentType(),
              DataHubUpgradeResult.class));
      versions.merge(urn, 1L, Long::sum);
    }

    @SuppressWarnings("unchecked")
    EntityService<?> entityService() throws Exception {
      EntityService<?> entityService = mock(EntityService.class);
      when(entityService.getLatestAspect(any(), any(Urn.class), any(String.class)))
          .thenAnswer(invocation -> rows.get((Urn) invocation.getArgument(1)));
      when(entityService.getLatestEnvelopedAspect(any(), any(), any(Urn.class), any()))
          .thenAnswer(
              invocation -> {
                EnvelopedAspect enveloped = readLatest(null, invocation.getArgument(2));
                if (enveloped == null) {
                  return null;
                }
                EnvelopedAspect withVersion = new EnvelopedAspect();
                withVersion.setValue(new Aspect(enveloped.getValue().data()));
                withVersion.setSystemMetadata(
                    new SystemMetadata().setVersion(enveloped.getSystemMetadata().getVersion()));
                return withVersion;
              });
      when(entityService.ingestProposal(
              any(), any(MetadataChangeProposal.class), any(), anyBoolean()))
          .thenAnswer(
              invocation -> {
                ingest(null, invocation.getArgument(1));
                return mock(IngestResult.class);
              });
      return entityService;
    }
  }

  static String usageAlias(String prefix) {
    return prefix + "_" + Constants.DATAHUB_USAGE_EVENT_INDEX;
  }

  static List<String> names(Iterable<ClusterHttp.AliasMember> members) {
    List<String> names = new ArrayList<>();
    for (ClusterHttp.AliasMember member : members) {
      names.add(member.index());
    }
    return names;
  }
}
