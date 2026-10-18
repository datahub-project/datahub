package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_STRUCT_PROPS_DISABLED;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeContext;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeReport;
import com.linkedin.datahub.upgrade.loadindices.AspectIndexLoader;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.EntityServiceConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.config.search.ComponentClusterConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchClusterIndexSettings;
import com.linkedin.metadata.config.search.SearchClusterSettings;
import com.linkedin.metadata.entity.EntityServiceAspectRetriever;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.PassThroughScopedTransactionFactory;
import com.linkedin.metadata.entity.ebean.PlainAspectTableResolver;
import com.linkedin.metadata.entity.storage.PrimaryStorageResolver;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultStore;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.OpenSearchSearchClientShim;
import com.linkedin.metadata.search.elasticsearch.index.DelegatingSettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.UpdateIndicesV3Strategy;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.V3DocumentIdScheme;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.service.IncrementalReindexWriteTargets;
import com.linkedin.metadata.service.UpdateGraphIndicesService;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.service.UpdateIndicesV2Strategy;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.SearchTestUtils;
import io.ebean.Database;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testng.SkipException;

/**
 * One or two OpenSearch clusters, an H2 aspect store, and every component of the Search V3
 * migration wired the way GMS and system update wire them, minus Kafka: change logs the entity
 * service produces are handed to an update-indices service in process.
 *
 * <p>{@link Topology} says where V3 lives: on its own cluster, which is what {@code
 * elasticsearch.componentCluster.searchV3=secondary} configures in a deployment, or on the cluster
 * V2 uses. {@link Generations} says which generations the deployment runs: V2 and V3 together, the
 * cutover shape, or V3 alone with keyword reads on from the first boot, the fresh-install shape.
 *
 * <p>Produced change logs are delivered to one of three consumer shapes. {@link
 * #deliverToV2OnlyConsumers()} models the corpus that already exists before V3 is switched on,
 * {@link #deliverToUpgradedConsumers()} models the consumers of the deploy that enabled V3, and
 * {@link #restartDualWritingConsumers(String)} models the consumers of a later deploy, brought up
 * before or after its Phase 1 ran, which mirror live deletes into the index a fill is building and
 * keep a swapped-out backing index current for a rollback.
 */
final class SearchV3MigrationHarness implements AutoCloseable {

  /** Which cluster serves V3. */
  enum Topology {
    SEPARATE_CLUSTERS,
    SAME_CLUSTER
  }

  /** Which entity index generations the deployment runs. */
  enum Generations {
    V2_AND_V3,
    V3_ONLY
  }

  /** Set to {@code true} to run these tests; they start OpenSearch containers. */
  static final String OPT_IN = "SEARCH_V3_MIGRATION_TEST";

  /**
   * Deploy-wide index prefix, as {@code INDEX_PREFIX} would set it. Deliberately different from
   * {@link #SECONDARY_INDEX_PREFIX}: every V2 name and, on one cluster, every V3 name carries this
   * one, while a split deployment names V3 under the other, so any path that resolves the wrong
   * prefix for a family shows up as a wrong name or a failed alias check.
   */
  static final String DEPLOY_INDEX_PREFIX = "acme";

  private static final String OPENSEARCH_IMAGE = "opensearchproject/opensearch";
  private static final String OPENSEARCH_VERSION = "2.19.3";
  private static final String OPENSEARCH_HEAP = "512m";
  static final String SECONDARY_INDEX_PREFIX = "tenant";
  private static final int HTTP_PORT = 9200;
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration CLIENT_CONNECT_TIMEOUT = Duration.ofSeconds(60);

  /** Longer than the consumers' watchful poll interval, so a planned fill is seen by one tick. */
  private static final Duration FILL_TARGET_POLL_TIMEOUT = Duration.ofSeconds(45);

  private static final long CLIENT_CONNECT_RETRY_MILLIS = 500;
  private static final Duration SYNC_TIMEOUT = Duration.ofMinutes(1);
  private static final IndicesOptions LENIENT = IndicesOptions.fromOptions(true, true, true, false);
  private static final String URN_FIELD = "urn";

  final Topology topology;
  final Generations generations;

  /** The cluster that serves V2 entity indices. */
  final SearchClientShim<?> v2Client;

  /**
   * The cluster that serves V3 entity indices; the same object as {@link #v2Client} when shared.
   */
  final SearchClientShim<?> v3Client;

  final ESBulkProcessor v2BulkProcessor;
  final ESBulkProcessor v3BulkProcessor;
  final ElasticSearchConfiguration config;
  final BuildIndicesConfiguration buildIndicesConfig;
  final IndexConvention indexConvention;
  final EntityRegistry entityRegistry;
  final SearchClusterRegistry clusterRegistry;
  final Database database;
  final EbeanAspectDao aspectDao;
  final ElasticSearchService searchService;
  final UpdateIndicesService updateIndicesService;
  final EntityServiceImpl entityService;
  final OperationContext opContext;
  private final List<GenericContainer<?>> containers;
  @Nullable private final UpdateIndicesService v2OnlyUpdateIndicesService;
  private final ESIndexBuilder v3IndexBuilder;
  private final V3MappingOverride v3MappingOverride;
  private final DelegatingSettingsBuilder settingsBuilder;
  private final SearchDocumentTransformer transformer;
  private final TimeseriesAspectService timeseriesAspectService;
  @Nullable private final UpdateIndicesStrategy v2Strategy;
  private final Sha256UrnEntityDocumentIdHasher documentIdHasher =
      new Sha256UrnEntityDocumentIdHasher();
  private final InProcessChangeLogProducer producer;
  @Nullable private IncrementalReindexWriteTargets writeTargets;

  /** Skips the calling test unless it was opted into and Docker can run the containers. */
  static void requireOptIn() {
    if (!"true".equalsIgnoreCase(System.getenv(OPT_IN))) {
      throw new SkipException(
          "Search V3 migration tests are opt-in: set "
              + OPT_IN
              + "=true or run :datahub-upgrade:searchV3MigrationTest");
    }
    if (!DockerClientFactory.instance().isDockerAvailable()) {
      throw new SkipException("Docker is not available; the tests need OpenSearch containers");
    }
  }

  static SearchV3MigrationHarness start(
      @Nonnull Topology topology, @Nonnull Generations generations) throws Exception {
    List<GenericContainer<?>> containers = new ArrayList<>();
    containers.add(openSearch());
    if (topology == Topology.SEPARATE_CLUSTERS) {
      containers.add(openSearch());
    }
    Startables.deepStart(containers.stream()).join();
    Database database =
        EbeanTestUtils.createNamedTestServer(
            "searchv3migration",
            "searchv3migration_" + UUID.randomUUID().toString().replace("-", ""));
    return new SearchV3MigrationHarness(topology, generations, containers, database);
  }

  /**
   * The next deploy, with {@code elasticsearch.componentCluster.searchV3} pointed at the other
   * cluster: new process wiring over the same containers and the same primary storage. A deployment
   * that ran both generations on one cluster gains a second cluster for V3; one that ran V3 apart
   * moves it onto the V2 cluster. Nothing on the cluster V3 leaves is touched. Only the harness
   * returned here should be closed; it owns every container, including the one left behind.
   */
  SearchV3MigrationHarness redeployWithV3OnTheOtherCluster() throws Exception {
    if (separateClusters()) {
      return new SearchV3MigrationHarness(Topology.SAME_CLUSTER, generations, containers, database);
    }
    GenericContainer<?> secondary = openSearch();
    secondary.start();
    List<GenericContainer<?>> withSecondCluster = new ArrayList<>(containers);
    withSecondCluster.add(secondary);
    return new SearchV3MigrationHarness(
        Topology.SEPARATE_CLUSTERS, generations, withSecondCluster, database);
  }

  private SearchV3MigrationHarness(
      Topology topology,
      Generations generations,
      List<GenericContainer<?>> containers,
      Database database)
      throws Exception {
    this.topology = topology;
    this.generations = generations;
    this.containers = containers;
    this.database = database;
    boolean v2Enabled = generations == Generations.V2_AND_V3;

    GenericContainer<?> primary = containers.get(0);
    GenericContainer<?> secondary = separateClusters() ? containers.get(1) : null;
    config =
        configuration(
            topology, generations, httpUri(primary), secondary == null ? null : httpUri(secondary));
    buildIndicesConfig = config.getBuildIndices();

    v2Client = client(primary);
    v2BulkProcessor = bulkProcessor(v2Client);
    ESIndexBuilder v2IndexBuilder = indexBuilder(v2Client);
    Map<String, SearchClusterRegistry.ClusterConnection> connections = new LinkedHashMap<>();
    connections.put(
        "primary",
        new SearchClusterRegistry.ClusterConnection(
            "primary", config, v2Client, v2BulkProcessor, v2IndexBuilder));
    if (secondary != null) {
      v3Client = client(secondary);
      v3BulkProcessor = bulkProcessor(v3Client);
      v3IndexBuilder = indexBuilder(v3Client);
      connections.put(
          "secondary",
          new SearchClusterRegistry.ClusterConnection(
              "secondary", config, v3Client, v3BulkProcessor, v3IndexBuilder));
    } else {
      v3Client = v2Client;
      v3BulkProcessor = v2BulkProcessor;
      v3IndexBuilder = v2IndexBuilder;
    }
    clusterRegistry = new SearchClusterRegistry(config, connections);
    entityRegistry = TestOperationContexts.defaultEntityRegistry();
    indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver(DEPLOY_INDEX_PREFIX),
            config.getEntityIndex(),
            IndexConventionImpl.explicitComponentPrefixOverlays(config));

    aspectDao =
        new EbeanAspectDao(
            PrimaryStorageResolver.forSingleEbeanDatabase(database),
            EbeanConfiguration.testDefault,
            null,
            List.of(),
            null,
            new PlainAspectTableResolver(),
            new PassThroughScopedTransactionFactory(database));
    aspectDao.setConnectionValidated(true);

    // Search side. The V3 builders take the V3 cluster's client: that is where V3 indices live.
    v3MappingOverride =
        new V3MappingOverride(
            SearchTestUtils.createDelegatingMappingsBuilder(config.getEntityIndex()));
    settingsBuilder =
        SearchTestUtils.createDelegatingSettingsBuilder(
            config.getEntityIndex(), config.getIndex(), indexConvention);
    ESSearchDAO searchDAO =
        new ESSearchDAO(
            false, config, null, QueryFilterRewriteChain.EMPTY, TEST_SEARCH_SERVICE_CONFIG);
    ESBrowseDAO browseDAO =
        new ESBrowseDAO(config, null, QueryFilterRewriteChain.EMPTY, TEST_SEARCH_SERVICE_CONFIG);
    ESWriteDAO writeDAO = new ESWriteDAO(config, v2Client, v2BulkProcessor, clusterRegistry);
    searchService =
        new ElasticSearchService(
            v2IndexBuilder,
            TEST_SEARCH_SERVICE_CONFIG,
            config,
            v3MappingOverride,
            settingsBuilder,
            clusterRegistry.entityIndexBuilderResolver(indexConvention),
            searchDAO,
            browseDAO,
            writeDAO);

    transformer = new SearchDocumentTransformer(1000, 1000, 4096, false, ESUtils.KEYWORD_MAXLENGTH);
    timeseriesAspectService = mock(TimeseriesAspectService.class);
    v2Strategy =
        v2Enabled
            ? new UpdateIndicesV2Strategy(
                config.getEntityIndex().getV2(),
                searchService,
                transformer,
                timeseriesAspectService,
                "MD5",
                null,
                indexConvention,
                false,
                new V2MappingsBuilder(
                    config.getEntityIndex(), OpenSearchSearchClientShim.PARTIAL_NGRAM_CONFIG),
                null)
            : null;
    updateIndicesService = updateIndices(enabledStrategies(v3Strategy(null)));
    v2OnlyUpdateIndicesService = v2Enabled ? updateIndices(List.of(v2Strategy)) : null;

    producer = new InProcessChangeLogProducer();
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    entityService =
        new EntityServiceImpl(
            aspectDao,
            producer,
            preProcessHooks,
            new EntityServiceConfiguration().setAlwaysEmitChangeLog(false).setEnableBrowseV2(true),
            null);

    opContext =
        TestOperationContexts.systemContext(
            null,
            null,
            null,
            () -> entityRegistry,
            () ->
                RetrieverContext.builder()
                    .aspectRetriever(
                        EntityServiceAspectRetriever.builder()
                            .entityService(entityService)
                            .entityRegistry(entityRegistry)
                            .build())
                    .cachingAspectRetriever(
                        TestOperationContexts.emptyActiveUsersAspectRetriever(() -> entityRegistry))
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .build(),
            () ->
                SearchContext.builder()
                    .indexConvention(indexConvention)
                    .searchableFieldTypes(
                        ESUtils.buildSearchableFieldTypes(entityRegistry, v3MappingOverride))
                    .searchableFieldPaths(ESUtils.buildSearchableFieldPaths(entityRegistry))
                    .searchClusterAccess(clusterRegistry)
                    .build(),
            context ->
                ((EntityServiceAspectRetriever) context.getAspectRetriever())
                    .setSystemOperationContext(context),
            null);
  }

  boolean separateClusters() {
    return topology == Topology.SEPARATE_CLUSTERS;
  }

  private List<UpdateIndicesStrategy> enabledStrategies(UpdateIndicesStrategy v3Strategy) {
    return v2Strategy == null ? List.of(v3Strategy) : List.of(v2Strategy, v3Strategy);
  }

  /** A fresh loader wired the way the upgrade job wires its bean. */
  AspectIndexLoader aspectIndexLoader() {
    return new AspectIndexLoader(aspectDao, updateIndicesService);
  }

  private UpdateIndicesStrategy v3Strategy(
      @Nullable IncrementalReindexWriteTargets rollbackTargets) {
    return UpdateIndicesV3Strategy.builder()
        .v3Config(config.getEntityIndex().getV3())
        .elasticSearchService(searchService)
        .searchDocumentTransformer(transformer)
        .timeseriesAspectService(timeseriesAspectService)
        .entityDocumentIdHasher(documentIdHasher)
        .documentContributors(List.of())
        .v2Enabled(v2Strategy != null)
        .incrementalReindexWriteTargets(rollbackTargets)
        .build();
  }

  private UpdateIndicesService updateIndices(List<UpdateIndicesStrategy> strategies) {
    return new UpdateIndicesService(
        mock(UpdateGraphIndicesService.class),
        searchService,
        mock(SystemMetadataService.class),
        strategies,
        null,
        true,
        true,
        true);
  }

  /** Produced change logs reach the V2 generation only: the corpus as it exists before V3. */
  void deliverToV2OnlyConsumers() {
    if (v2OnlyUpdateIndicesService == null) {
      throw new IllegalStateException("V2 is not enabled in this deployment shape");
    }
    producer.deliverTo(v2OnlyUpdateIndicesService);
  }

  /** Produced change logs reach every enabled generation: the consumers that came with V3. */
  void deliverToUpgradedConsumers() {
    producer.deliverTo(updateIndicesService);
  }

  /**
   * The consumers of the deploy {@code upgradeVersion}, brought up whenever the test calls this:
   * they read that deploy's state at start, poll it afterwards as the real consumers do, repeat
   * every V3 write into a swapped-out backing index it records, so the alias can be swapped back
   * without losing what arrived since, and mirror live deletes into the index a fill is building.
   */
  void restartDualWritingConsumers(@Nonnull String upgradeVersion) {
    if (writeTargets != null) {
      writeTargets.close();
    }
    writeTargets =
        IncrementalReindexWriteTargets.fromUpgradeResult(
            new IncrementalReindexWriteTargets.StateSource(
                opContext,
                upgradeResultStore(),
                upgradeUrn(IncrementalReindexState.UPGRADE_ID_PREFIX, upgradeVersion)),
            null,
            true);
    producer.deliverTo(updateIndices(enabledStrategies(v3Strategy(writeTargets))));
  }

  /**
   * Waits until the consumers' poll has picked up the index a fill is building for {@code alias},
   * as consumers that were serving before the fill was planned have to.
   */
  void awaitFillTarget(@Nonnull String alias) throws InterruptedException {
    if (writeTargets == null) {
      throw new IllegalStateException("no consumers have been restarted on a deploy's version");
    }
    long deadline = System.currentTimeMillis() + FILL_TARGET_POLL_TIMEOUT.toMillis();
    while (writeTargets.fillTargetFor(alias).isEmpty()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError(
            "the consumers did not pick up a fill target for " + alias + " by polling");
      }
      Thread.sleep(250);
    }
  }

  /** Changes one field's mapping on a V3 index, as a release that evolves the schema would. */
  void overrideV3FieldMapping(
      @Nonnull String indexName, @Nonnull String field, @Nonnull Map<String, Object> mapping) {
    v3MappingOverride.override(indexName, field, mapping);
  }

  void ingest(@Nonnull Urn urn, @Nonnull String aspectName, @Nonnull RecordTemplate aspect) {
    entityService.ingestAspects(
        opContext,
        urn,
        List.of(Pair.of(aspectName, aspect)),
        AuditStampUtils.createDefaultAuditStamp(),
        SystemMetadataUtils.createDefaultSystemMetadata());
  }

  /** Drains every cluster's bulk processor so every produced write is searchable. */
  void syncWrites() throws InterruptedException, TimeoutException {
    v2BulkProcessor.flushAndWait(SYNC_TIMEOUT);
    v3BulkProcessor.flushAndWait(SYNC_TIMEOUT);
  }

  UpgradeContext upgradeContext() {
    Upgrade upgrade =
        new Upgrade() {
          @Override
          public String id() {
            return "SearchV3Migration";
          }

          @Override
          public List<UpgradeStep> steps() {
            return List.of();
          }
        };
    return new DefaultUpgradeContext(
        opContext, upgrade, new DefaultUpgradeReport(), List.of(), List.of());
  }

  /** The V3 alias of {@code entityName}'s index: one index per entity type. */
  String v3Alias(@Nonnull String entityName) {
    return indexConvention.getEntityIndexNameV3(
        opContext, V3IndexKeys.resolve(entityRegistry.getEntitySpec(entityName)));
  }

  String v2Index(@Nonnull String entityName) {
    return indexConvention.getEntityIndexName(opContext, entityName);
  }

  Map<String, String> phase1State(@Nonnull String upgradeVersion) {
    return upgradeState(IncrementalReindexState.UPGRADE_ID_PREFIX, upgradeVersion);
  }

  Map<String, String> catchUpState(@Nonnull String upgradeVersion) {
    return upgradeState(IncrementalReindexState.CATCH_UP_UPGRADE_ID_PREFIX, upgradeVersion);
  }

  private Map<String, String> upgradeState(@Nonnull String prefix, @Nonnull String upgradeVersion) {
    return BootstrapStep.getUpgradeResult(
            opContext, upgradeUrn(prefix, upgradeVersion), entityService)
        .map(DataHubUpgradeResult::getResult)
        .<Map<String, String>>map(LinkedHashMap::new)
        .orElseGet(LinkedHashMap::new);
  }

  private static Urn upgradeUrn(@Nonnull String prefix, @Nonnull String upgradeVersion) {
    return BootstrapStep.getUpgradeUrn(prefix + "_" + upgradeVersion);
  }

  Set<String> backingIndices(@Nonnull String alias) throws IOException {
    return v3IndexBuilder.getBackingIndices(opContext, alias);
  }

  /** Points {@code alias} at {@code backingIndex}, keeping the index it pointed at before. */
  void swapAlias(@Nonnull String alias, @Nonnull String backingIndex) throws Exception {
    ESIndexBuilder.renameReindexedIndices(
        v3Client, opContext, alias, null, backingIndex, false, RequestOptions.DEFAULT);
  }

  /**
   * Replaces whatever backs {@code alias} with {@code unstampedIndex}: the current target mapping
   * without the document-id scheme stamp, holding the same documents under the ids {@code
   * documentIdOf} derives from their URNs. With URL-encoded URN ids that is a V3 index built before
   * document ids were hashed; with hashed ids it is one a writer created before stamping existed.
   * When {@code unstampedIndex} is the alias name itself, the index takes that name directly, as a
   * V3 index created before indices were aliased did.
   */
  void replaceWithUnstampedIndex(
      @Nonnull String alias,
      @Nonnull String unstampedIndex,
      @Nonnull Function<String, String> documentIdOf)
      throws Exception {
    List<SearchHit> documents = hits(v3Client, alias);
    Map<String, Object> unstamped =
        new HashMap<>(
            v3MappingOverride.getIndexMappings(opContext).stream()
                .filter(indexMapping -> indexMapping.getIndexName().equals(alias))
                .findFirst()
                .orElseThrow()
                .getMappings());
    unstamped.remove(V3DocumentIdScheme.META);
    Map<String, Object> settings = settingsBuilder.getSettings(config.getIndex(), alias);

    Set<String> current = backingIndices(alias);
    v3Client.deleteIndex(
        opContext, new DeleteIndexRequest(current.toArray(new String[0])), RequestOptions.DEFAULT);
    v3IndexBuilder.createIndex(
        opContext,
        unstampedIndex,
        v3IndexBuilder.buildReindexState(opContext, unstampedIndex, unstamped, settings));
    if (!unstampedIndex.equals(alias)) {
      v3Client.updateIndexAliases(
          opContext,
          new IndicesAliasesRequest()
              .addAliasAction(
                  IndicesAliasesRequest.AliasActions.add().index(unstampedIndex).alias(alias)),
          RequestOptions.DEFAULT);
    }
    for (SearchHit document : documents) {
      String urn = urnOf(document);
      v3BulkProcessor.add(
          opContext,
          urn,
          new IndexRequest(unstampedIndex)
              .id(documentIdOf.apply(urn))
              .source(document.getSourceAsMap()));
    }
    v3BulkProcessor.flushAndWait(SYNC_TIMEOUT);
  }

  /** The document id every V3 write uses today. */
  String hashedDocumentId(@Nonnull Urn urn) {
    return documentIdHasher.documentId(opContext, urn);
  }

  /** The document id V3 indices used before hashing. */
  static String legacyDocumentId(@Nonnull String urn) {
    return URLEncoder.encode(urn, StandardCharsets.UTF_8);
  }

  static String urnOf(@Nonnull SearchHit hit) {
    return (String) hit.getSourceAsMap().get(URN_FIELD);
  }

  boolean indexExists(@Nonnull SearchClientShim<?> client, @Nonnull String name)
      throws IOException {
    return client.indexExists(opContext, new GetIndexRequest(name), RequestOptions.DEFAULT);
  }

  long count(
      @Nonnull SearchClientShim<?> client, @Nonnull String index, @Nonnull QueryBuilder query)
      throws IOException {
    SearchRequest request = new SearchRequest(index).indicesOptions(LENIENT);
    request.source(new SearchSourceBuilder().size(0).trackTotalHits(true).query(query));
    return client.search(opContext, request, RequestOptions.DEFAULT).getHits().getTotalHits().value;
  }

  long count(@Nonnull SearchClientShim<?> client, @Nonnull String index) throws IOException {
    return count(client, index, QueryBuilders.matchAllQuery());
  }

  /** Every document in {@code index}. */
  List<SearchHit> hits(@Nonnull SearchClientShim<?> client, @Nonnull String index)
      throws IOException {
    SearchRequest request = new SearchRequest(index).indicesOptions(LENIENT);
    request.source(new SearchSourceBuilder().size(1000).query(QueryBuilders.matchAllQuery()));
    return Arrays.asList(
        client.search(opContext, request, RequestOptions.DEFAULT).getHits().getHits());
  }

  /** Each document's id in {@code index}, keyed by URN. */
  Map<String, String> idsByUrn(@Nonnull SearchClientShim<?> client, @Nonnull String index)
      throws IOException {
    return hits(client, index).stream()
        .collect(Collectors.toMap(SearchV3MigrationHarness::urnOf, SearchHit::getId));
  }

  @Override
  public void close() {
    if (writeTargets != null) {
      writeTargets.close();
    }
    for (GenericContainer<?> container : containers) {
      try {
        container.stop();
      } catch (RuntimeException e) {
        // best effort teardown
      }
    }
    database.shutdown();
  }

  /** The upgrade-result store GMS and the MAE consumer read Phase 1 state through. */
  private DataHubUpgradeResultStore upgradeResultStore() {
    return new DataHubUpgradeResultStore() {
      @Override
      @Nullable
      public EnvelopedAspect readLatest(OperationContext context, @Nonnull Urn upgradeIdUrn)
          throws Exception {
        return entityService.getLatestEnvelopedAspect(
            context,
            Constants.DATA_HUB_UPGRADE_ENTITY_NAME,
            upgradeIdUrn,
            Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);
      }

      @Override
      public void ingest(OperationContext context, @Nonnull MetadataChangeProposal proposal) {
        entityService.ingestProposal(
            context, proposal, AuditStampUtils.createDefaultAuditStamp(), false);
      }
    };
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static GenericContainer<?> openSearch() {
    String image = System.getenv("OPENSEARCH_IMAGE_FULL_NAME");
    if (image == null || image.isEmpty()) {
      image = OPENSEARCH_IMAGE + ":" + OPENSEARCH_VERSION;
    }
    GenericContainer container =
        new OpensearchContainer(
            DockerImageName.parse(image).asCompatibleSubstituteFor(OPENSEARCH_IMAGE));
    container
        .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms" + OPENSEARCH_HEAP + " -Xmx" + OPENSEARCH_HEAP)
        .withEnv("DISABLE_SECURITY_PLUGIN", "true")
        .withStartupTimeout(STARTUP_TIMEOUT);
    return container;
  }

  private static String httpUri(GenericContainer<?> container) {
    return "http://" + container.getHost() + ":" + container.getMappedPort(HTTP_PORT);
  }

  /**
   * Right after a container starts, a connection to its mapped port from this JVM can be refused
   * even though the library's own HTTP wait connected to it just before; under heavy load the
   * refusal has been seen to outlast a minute. The shim's engine detection probes once and does not
   * retry, so the harness retries it for a bounded time and otherwise fails with the probe's error.
   */
  private static SearchClientShim<?> client(GenericContainer<?> container) throws IOException {
    SearchClientShim.ShimConfiguration configuration =
        new SearchClientShimUtil.ShimConfigurationBuilder()
            .withHost(container.getHost())
            .withSSL(false)
            .withPort(container.getMappedPort(HTTP_PORT))
            .withThreadCount(1)
            .withConnectionRequestTimeout(30000)
            .build();
    Instant deadline = Instant.now().plus(CLIENT_CONNECT_TIMEOUT);
    while (true) {
      try {
        return SearchClientShimUtil.createShimWithAutoDetection(configuration, new ObjectMapper());
      } catch (IOException e) {
        // A container that is no longer running never answers; say so, with the exit state the
        // engine recorded, instead of retrying against it.
        if (!container.isRunning()) {
          InspectContainerResponse.ContainerState state =
              container.getCurrentContainerInfo().getState();
          throw new IOException(
              "Container for "
                  + configuration.getHost()
                  + ":"
                  + configuration.getPort()
                  + " is not running (status="
                  + state.getStatus()
                  + ", exitCode="
                  + state.getExitCodeLong()
                  + ", oomKilled="
                  + state.getOOMKilled()
                  + ")",
              e);
        }
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
        .bulkRequestsLimit(1000)
        .bulkFlushPeriod(1)
        .retryInterval(1L)
        .numRetries(1)
        .build();
  }

  private static ESIndexBuilder indexBuilder(SearchClientShim<?> client) {
    return new ESIndexBuilder(
        client,
        TEST_OS_SEARCH_CONFIG,
        TEST_ES_STRUCT_PROPS_DISABLED,
        Map.of(),
        new GitVersion("0.0.0-test", "123456", Optional.empty()));
  }

  /**
   * The test search configuration for the requested shape, with the incremental reindex machinery
   * enabled. With both generations, V3 is on, keyword reads start on V2 and the read-flip scenario
   * turns them on through the mutable configuration objects the readers hold. With V3 alone, V2 is
   * off and keyword reads are on from the start.
   */
  private static ElasticSearchConfiguration configuration(
      Topology topology,
      Generations generations,
      String primaryUri,
      @Nullable String secondaryUri) {
    boolean v2Enabled = generations == Generations.V2_AND_V3;
    boolean readsOnV3 = generations == Generations.V3_ONLY;
    Map<String, SearchClusterSettings> clusters = new LinkedHashMap<>();
    clusters.put("primary", SearchClusterSettings.builder().uri(primaryUri).build());
    if (secondaryUri != null) {
      // A shared or tenant-scoped remote cluster names its indices under its own prefix; V3
      // naming must follow that overlay, not the deploy-wide prefix, in both directions.
      clusters.put(
          "secondary",
          SearchClusterSettings.builder()
              .uri(secondaryUri)
              .index(SearchClusterIndexSettings.builder().prefix(SECONDARY_INDEX_PREFIX).build())
              .build());
    }
    return TEST_OS_SEARCH_CONFIG.toBuilder()
        .clusters(clusters)
        .componentCluster(
            ComponentClusterConfiguration.builder()
                .searchV2("primary")
                .searchV3(topology == Topology.SEPARATE_CLUSTERS ? "secondary" : "primary")
                .semantic("primary")
                .graph("primary")
                .timeseries("primary")
                .systemMetadata("primary")
                .usage("primary")
                .build())
        .buildIndices(
            TEST_OS_SEARCH_CONFIG.getBuildIndices().toBuilder()
                .incrementalReindexEnabled(true)
                .catchUpSqlPageSize(50)
                .catchUpFlushInterval(500)
                .catchUpFlushBytesThreshold(0L)
                .backfillBatchSize(100)
                .maxBackfillHours(0)
                .build())
        .entityIndex(
            EntityIndexConfiguration.builder()
                .v2(
                    EntityIndexVersionConfiguration.builder()
                        .enabled(v2Enabled)
                        .cleanup(false)
                        .build())
                .v3(
                    EntityIndexVersionConfiguration.builder()
                        .enabled(true)
                        .cleanup(false)
                        .keywordReadEnabled(readsOnV3)
                        .analyzerConfig("search_entity_analyzer_config.yaml")
                        .mappingConfig("search_entity_mapping_config.yaml")
                        .maxFieldsLimit(5000)
                        .build())
                .build())
        .build();
  }

  /**
   * Lets a scenario change what a release's V3 mapping says about a field, on one index, between
   * system-update runs. Only {@code properties} is touched, so the document-id scheme stamp the
   * wrapped builder records stays as it is.
   */
  private static final class V3MappingOverride implements MappingsBuilder {

    private static final ObjectMapper COPIER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {};

    private final MappingsBuilder delegate;
    private final Map<String, Map<String, Map<String, Object>>> fieldsByIndex =
        new ConcurrentHashMap<>();

    V3MappingOverride(@Nonnull MappingsBuilder delegate) {
      this.delegate = delegate;
    }

    void override(
        @Nonnull String indexName, @Nonnull String field, @Nonnull Map<String, Object> mapping) {
      fieldsByIndex
          .computeIfAbsent(indexName, name -> new ConcurrentHashMap<>())
          .put(field, mapping);
    }

    @Override
    public Collection<IndexMapping> getIndexMappings(
        @Nonnull OperationContext opContext,
        @Nonnull Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
      return applied(delegate.getIndexMappings(opContext, structuredProperties));
    }

    @Override
    public Collection<IndexMapping> getIndexMappingsWithNewStructuredProperty(
        @Nonnull OperationContext opContext,
        @Nonnull Urn urn,
        @Nonnull StructuredPropertyDefinition property) {
      return applied(delegate.getIndexMappingsWithNewStructuredProperty(opContext, urn, property));
    }

    @Override
    public Map<String, Object> getIndexMappingsForStructuredProperty(
        Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
      return delegate.getIndexMappingsForStructuredProperty(properties);
    }

    @Override
    public Map<String, Object> getIndexMappings(
        @Nonnull EntityRegistry entityRegistry, @Nonnull EntitySpec entitySpec) {
      return delegate.getIndexMappings(entityRegistry, entitySpec);
    }

    @SuppressWarnings("unchecked")
    private List<IndexMapping> applied(@Nonnull Collection<IndexMapping> indexMappings) {
      List<IndexMapping> result = new ArrayList<>();
      for (IndexMapping indexMapping : indexMappings) {
        Map<String, Map<String, Object>> fields = fieldsByIndex.get(indexMapping.getIndexName());
        if (fields == null) {
          result.add(indexMapping);
          continue;
        }
        Map<String, Object> mappings = COPIER.convertValue(indexMapping.getMappings(), MAP);
        ((Map<String, Object>) mappings.get(ESUtils.PROPERTIES)).putAll(fields);
        result.add(
            IndexMapping.builder()
                .indexName(indexMapping.getIndexName())
                .mappings(mappings)
                .build());
      }
      return result;
    }
  }

  /**
   * Stands in for Kafka: every change log the entity service produces is handed to the configured
   * update-indices service synchronously, so the test can assert index state right after a write.
   * With no target set, change logs are dropped, which is how the aspect store is seeded without
   * touching any index.
   */
  private static final class InProcessChangeLogProducer extends EventProducer {

    private final AtomicReference<UpdateIndicesService> target = new AtomicReference<>();

    void deliverTo(@Nullable UpdateIndicesService updateIndicesService) {
      target.set(updateIndicesService);
    }

    @Override
    public void flush() {}

    @Override
    public Future<?> produceMCL(
        @Nonnull OperationContext opContext,
        @Nonnull Urn urn,
        @Nullable AspectSpec aspectSpec,
        @Nonnull MetadataChangeLog metadataChangeLog) {
      UpdateIndicesService updateIndicesService = target.get();
      if (updateIndicesService != null) {
        updateIndicesService.handleChangeEvents(opContext, List.of(metadataChangeLog));
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public String getMetadataChangeLogTopicName(@Nonnull AspectSpec aspectSpec) {
      return "MetadataChangeLog_Versioned_v1";
    }

    @Override
    protected Future<?> doProduceMetadataChangeProposal(
        @Nonnull OperationContext opContext,
        @Nonnull Urn urn,
        @Nonnull MetadataChangeProposal metadataChangeProposal) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public String getMetadataChangeProposalTopicName() {
      return "MetadataChangeProposal_v1";
    }

    @Override
    public Future<?> produceFailedMetadataChangeProposalAsync(
        @Nonnull OperationContext opContext,
        @Nonnull MetadataChangeProposal mcp,
        @Nonnull Set<Throwable> throwables) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<?> producePlatformEvent(
        @Nonnull OperationContext opContext,
        @Nonnull String name,
        @Nullable String key,
        @Nonnull PlatformEvent payload) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public String getPlatformEventTopicName() {
      return "PlatformEvent_v1";
    }
  }
}
