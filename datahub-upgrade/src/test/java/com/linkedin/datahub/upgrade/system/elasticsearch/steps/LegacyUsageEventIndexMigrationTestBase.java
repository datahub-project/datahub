package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.UsageEventIndexUtils;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.opensearch.client.Request;
import org.testcontainers.containers.GenericContainer;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Runs {@link CreateUsageEventIndicesStep} against a real engine holding a usage event index that
 * was auto-created by the first usage event write, before its index template existed. Such an index
 * maps {@code type} dynamically as {@code text}, so the audit event sort on it fails.
 */
public abstract class LegacyUsageEventIndexMigrationTestBase {

  protected static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);
  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final long TEST_TIMEOUT_MS = 180_000;
  private static final String AUDIT_SORT =
      "[{\"timestamp\":\"desc\"},{\"type\":\"asc\"},{\"actorUrn.keyword\":\"asc\"}]";

  private GenericContainer<?> container;
  private SearchClientShim<?> searchClient;

  protected abstract GenericContainer<?> createContainer();

  /** Where {@code _resolve/index} lists the managed layout: data_streams or aliases. */
  protected abstract String managedLayoutField();

  @BeforeClass(alwaysRun = true)
  public void startEngine() throws IOException {
    container = createContainer();
    container.start();
    searchClient =
        SearchClientShimUtil.createShimWithAutoDetection(
            new SearchClientShimUtil.ShimConfigurationBuilder()
                .withHost(container.getHost())
                .withPort(container.getMappedPort(9200))
                .withSSL(false)
                .withThreadCount(1)
                .withConnectionRequestTimeout(30000)
                .build(),
            OBJECT_MAPPER);
  }

  @AfterClass(alwaysRun = true)
  public void stopEngine() throws IOException {
    if (searchClient != null) {
      searchClient.close();
    }
    if (container != null) {
      container.stop();
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testLegacyIndexIsMovedOntoTheTemplateLayout() throws Exception {
    String prefix = "migrate_";
    String index = prefix + "datahub_usage_event";
    index(
        index,
        "1",
        "{\"type\":\"SearchEvent\",\"timestamp\":1756000000000,\"@timestamp\":1756000000000,"
            + "\"actorUrn\":\"urn:li:corpuser:a\"}");
    index(
        index,
        "2",
        "{\"type\":\"PageViewEvent\",\"timestamp\":1756000001000,"
            + "\"actorUrn\":\"urn:li:corpuser:a\"}");
    index(index, "3", "{\"type\":\"LogInEvent\",\"actorUrn\":\"urn:li:corpuser:b\"}");
    refresh(index);
    assertEquals(typeMappings(index), List.of("text"));

    assertEquals(runStep(prefix), DataHubUpgradeState.SUCCEEDED);

    JsonNode resolved = request("GET", "/_resolve/index/" + index, null);
    assertEquals(resolved.path("indices").size(), 0);
    assertTrue(names(resolved.path(managedLayoutField())).contains(index));
    List<String> typeMappings = typeMappings(index);
    assertFalse(typeMappings.isEmpty());
    assertTrue(typeMappings.stream().allMatch("keyword"::equals));
    refresh(index);
    // A data stream cannot take the event with no timestamp at all, so it is dropped there.
    Set<String> expectedIds = isDataStream() ? Set.of("1", "2") : Set.of("1", "2", "3");
    assertEquals(searchIds(index, "{\"size\":10,\"sort\":" + AUDIT_SORT + "}"), expectedIds);
    assertEquals(
        searchIds(index, "{\"query\":{\"term\":{\"type\":\"SearchEvent\"}}}"), Set.of("1"));
    assertEquals(legacyBackups(prefix), List.of());

    assertEquals(runStep(prefix), DataHubUpgradeState.SUCCEEDED);
    refresh(index);
    assertEquals(searchIds(index, "{\"size\":10}"), expectedIds);
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testLeftoverBackupIsBackfilled() throws Exception {
    String prefix = "resume_";
    String index = prefix + "datahub_usage_event";
    assertEquals(runStep(prefix), DataHubUpgradeState.SUCCEEDED);
    // What an interrupted migration leaves behind after swapping the layout in.
    String backup = prefix + "legacy_datahub_usage_event_1";
    index(
        backup,
        "7",
        "{\"type\":\"SearchEvent\",\"timestamp\":1756000007000,\"@timestamp\":1756000007000,"
            + "\"actorUrn\":\"urn:li:corpuser:c\"}");
    refresh(backup);

    assertEquals(runStep(prefix), DataHubUpgradeState.SUCCEEDED);

    refresh(index);
    assertEquals(searchIds(index, "{\"size\":10}"), Set.of("7"));
    assertEquals(legacyBackups(prefix), List.of());
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testBackupIsKeptWhenEventsCannotBeCopiedBack() throws Exception {
    String prefix = "partial_";
    String index = prefix + "datahub_usage_event";
    // The template maps browserId as keyword, so an object value cannot be copied back.
    index(
        index,
        "1",
        "{\"type\":\"SearchEvent\",\"timestamp\":1756000000000,\"@timestamp\":1756000000000,"
            + "\"actorUrn\":\"urn:li:corpuser:a\",\"browserId\":{\"id\":\"b1\"}}");
    refresh(index);

    assertEquals(runStep(prefix), DataHubUpgradeState.SUCCEEDED);

    assertEquals(request("GET", "/_resolve/index/" + index, null).path("indices").size(), 0);
    List<String> backups = legacyBackups(prefix);
    assertEquals(backups.size(), 1);
    String task = recordedBackfillTask(backups.get(0));
    assertFalse(task.isEmpty());

    // A later run checks the recorded copy instead of copying the backup again.
    assertEquals(runStep(prefix), DataHubUpgradeState.SUCCEEDED);
    assertEquals(legacyBackups(prefix), backups);
    assertEquals(recordedBackfillTask(backups.get(0)), task);
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testBackupWithAForgottenCopyIsCopiedAgainOnlyIntoTheSameWriteIndex()
      throws Exception {
    String prefix = "vanished_";
    String index = prefix + "datahub_usage_event";
    assertEquals(runStep(prefix), DataHubUpgradeState.SUCCEEDED);
    String backup = prefix + "legacy_datahub_usage_event_1";
    index(
        backup,
        "8",
        "{\"type\":\"SearchEvent\",\"timestamp\":1756000008000,\"@timestamp\":1756000008000,"
            + "\"actorUrn\":\"urn:li:corpuser:d\"}");
    refresh(backup);

    // The cluster no longer knows the recorded copy, and the index it went into has rolled over
    // since, so copying again could duplicate events: the backup is left alone.
    recordCopy(backup, "nonexistentnode:1", "rolled-over-index");
    assertEquals(runStep(prefix), DataHubUpgradeState.SUCCEEDED);
    assertEquals(legacyBackups(prefix), List.of(backup));

    // Recorded against the current write index, the copy is started again.
    recordCopy(backup, "nonexistentnode:1", writeIndex(index));
    assertEquals(runStep(prefix), DataHubUpgradeState.SUCCEEDED);
    refresh(index);
    assertEquals(searchIds(index, "{\"size\":10}"), Set.of("8"));
    assertEquals(legacyBackups(prefix), List.of());
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testMigrationWaitsWhileARecentCloneMayBelongToAnotherRun() throws Exception {
    String prefix = "retried_";
    String index = prefix + "datahub_usage_event";
    index(
        index,
        "1",
        "{\"type\":\"SearchEvent\",\"timestamp\":1756000000000,\"@timestamp\":1756000000000,"
            + "\"actorUrn\":\"urn:li:corpuser:a\"}");
    refresh(index);
    // A clone made moments ago, as by another attempt that may still be migrating the index.
    request("PUT", "/" + index + "/_block/write", null);
    String clone = prefix + "legacy_datahub_usage_event_1";
    request(
        "POST", "/" + index + "/_clone/" + clone, "{\"settings\":{\"index.blocks.write\":null}}");
    request("PUT", "/" + index + "/_settings", "{\"index.blocks.write\":false}");

    // OpenSearch still fails the step here, as it did before, until the index is migrated.
    runStep(prefix);

    assertEquals(request("GET", "/_resolve/index/" + index, null).path("indices").size(), 1);
    assertEquals(legacyBackups(prefix), List.of(clone));
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testTwoRunsAtOnceMoveTheIndexOnce() throws Exception {
    String prefix = "concurrent_";
    String index = prefix + "datahub_usage_event";
    indexLegacyEvents(index);
    AtomicInteger clones = new AtomicInteger();
    AtomicInteger copies = new AtomicInteger();
    SearchClientShim<?> client =
        clientWith(
            (request, real) -> {
              if (request.getEndpoint().contains("/_clone/")) {
                clones.incrementAndGet();
              }
              if (request.getEndpoint().startsWith("/_reindex")) {
                copies.incrementAndGet();
              }
              return real.call();
            });
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      List<Future<DataHubUpgradeState>> runs = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        CreateUsageEventIndicesStep step = newStep(prefix, client);
        runs.add(
            pool.submit(
                () -> {
                  start.await();
                  return run(step);
                }));
      }
      start.countDown();
      for (Future<DataHubUpgradeState> run : runs) {
        // The run that finds the lease taken still fails on OpenSearch while the index is plain.
        run.get();
      }
    } finally {
      pool.shutdown();
    }

    assertEquals(clones.get(), 1);
    assertEquals(copies.get(), 1);
    refresh(index);
    assertEquals(searchIds(index, "{\"size\":10}"), Set.of("1", "2"));
    assertEquals(legacyBackups(prefix), List.of());
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testALeaseHeldByAnotherRunLeavesTheIndexAlone() throws Exception {
    String prefix = "leased_";
    String index = prefix + "datahub_usage_event";
    indexLegacyEvents(index);
    createLease(prefix, "another-run");

    runStep(prefix);

    assertEquals(request("GET", "/_resolve/index/" + index, null).path("indices").size(), 1);
    assertEquals(leaseOwner(prefix), "another-run");
    assertEquals(legacyBackups(prefix), List.of(prefix + "legacy_datahub_usage_event_lease"));
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testAWriteBlockLeftByARunThatDiedHoldingTheLeaseIsLifted() throws Exception {
    String prefix = "blocked_lease_";
    String index = prefix + "datahub_usage_event";
    indexLegacyEvents(index);
    request("PUT", "/" + index + "/_block/write", null);
    createLease(prefix, "a-run-that-died");
    UsageEventIndexUtils.setBlockedIndexWaitForTesting(
        Duration.ofMillis(100), Duration.ofSeconds(1));
    try {
      runStep(prefix);
    } finally {
      UsageEventIndexUtils.clearBlockedIndexWaitForTesting();
    }

    // Usage events are accepted again, and the migration is left to a run that holds the lease.
    index(
        index,
        "3",
        "{\"type\":\"SearchEvent\",\"timestamp\":1756000003000,\"@timestamp\":1756000003000}");
    assertEquals(request("GET", "/_resolve/index/" + index, null).path("indices").size(), 1);
    assertEquals(leaseOwner(prefix), "a-run-that-died");
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testAStaleLeaseIsTakenOver() throws Exception {
    String prefix = "stale_lease_";
    String index = prefix + "datahub_usage_event";
    indexLegacyEvents(index);
    createLease(prefix, "a-run-that-died");
    UsageEventIndexUtils.setLeaseTtlForTesting(Duration.ZERO);
    try {
      assertEquals(runStep(prefix), DataHubUpgradeState.SUCCEEDED);
    } finally {
      UsageEventIndexUtils.clearLeaseTtlForTesting();
    }

    assertMigrated(prefix, Set.of("1", "2"));
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testAMoveThatFailsAfterRemovingTheOriginalIsRecovered() throws Exception {
    String prefix = "recovered_";
    String index = prefix + "datahub_usage_event";
    indexLegacyEvents(index);
    AtomicInteger failures = new AtomicInteger(isDataStream() ? 2 : 1);
    SearchClientShim<?> client =
        clientWith(
            (request, real) -> {
              String call = request.getMethod() + " " + request.getEndpoint();
              if (isDataStream()
                  && call.equals("PUT /_data_stream/" + index)
                  && failures.getAndDecrement() > 0) {
                throw new IOException("injected: data stream creation failed");
              }
              if (!isDataStream()
                  && call.equals("POST /_aliases")
                  && failures.getAndDecrement() > 0) {
                real.call();
                throw new IOException("injected: the alias swap happened but its answer was lost");
              }
              return real.call();
            });
    CreateUsageEventIndicesStep step = newStep(prefix, client);

    if (isDataStream()) {
      // The original is deleted and the data stream cannot be created, so the step fails with the
      // events kept in the backup...
      assertEquals(run(step), DataHubUpgradeState.FAILED);
      assertEquals(legacyBackups(prefix).size(), 1);
    }
    // ...and its retry creates the layout and copies the backup back.
    assertEquals(run(step), DataHubUpgradeState.SUCCEEDED);

    assertMigrated(prefix, Set.of("1", "2"));
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testALostAnswerToTheDeleteIsReconciledFromTheCluster() throws Exception {
    if (!isDataStream()) {
      throw new SkipException("OpenSearch swaps the alias atomically; the test above covers it");
    }
    String prefix = "lost_delete_";
    String index = prefix + "datahub_usage_event";
    indexLegacyEvents(index);
    AtomicBoolean once = new AtomicBoolean(true);
    SearchClientShim<?> client =
        clientWith(
            (request, real) -> {
              if ((request.getMethod() + " " + request.getEndpoint()).equals("DELETE /" + index)
                  && once.getAndSet(false)) {
                real.call();
                throw new IOException("injected: the delete happened but its answer was lost");
              }
              return real.call();
            });

    assertEquals(run(newStep(prefix, client)), DataHubUpgradeState.SUCCEEDED);

    assertMigrated(prefix, Set.of("1", "2"));
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testABackupWhoseCopyCannotStartFailsTheStep() throws Exception {
    String prefix = "unstarted_";
    String index = prefix + "datahub_usage_event";
    assertEquals(runStep(prefix), DataHubUpgradeState.SUCCEEDED);
    String backup = prefix + "legacy_datahub_usage_event_1";
    index(
        backup,
        "9",
        "{\"type\":\"SearchEvent\",\"timestamp\":1756000009000,\"@timestamp\":1756000009000,"
            + "\"actorUrn\":\"urn:li:corpuser:e\"}");
    refresh(backup);
    AtomicBoolean once = new AtomicBoolean(true);
    SearchClientShim<?> client =
        clientWith(
            (request, real) -> {
              if (request.getEndpoint().startsWith("/_reindex") && once.getAndSet(false)) {
                throw new IOException("injected: the copy was refused");
              }
              return real.call();
            });

    // The backup holds the only copy of its events, so the step fails instead of succeeding
    // without them.
    assertEquals(run(newStep(prefix, client)), DataHubUpgradeState.FAILED);
    assertEquals(legacyBackups(prefix), List.of(backup));

    assertEquals(runStep(prefix), DataHubUpgradeState.SUCCEEDED);
    refresh(index);
    assertEquals(searchIds(index, "{\"size\":10}"), Set.of("9"));
    assertEquals(legacyBackups(prefix), List.of());
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testALostAnswerToTheCopyIsFoundInsteadOfCopyingAgain() throws Exception {
    String prefix = "adopted_";
    String index = prefix + "datahub_usage_event";
    assertEquals(runStep(prefix), DataHubUpgradeState.SUCCEEDED);
    String backup = prefix + "legacy_datahub_usage_event_1";
    Set<String> ids = new HashSet<>();
    for (int i = 0; i < 20; i++) {
      long timestamp = 1756000000000L + i;
      index(
          backup,
          "e" + i,
          "{\"type\":\"SearchEvent\",\"timestamp\":"
              + timestamp
              + ",\"@timestamp\":"
              + timestamp
              + ",\"actorUrn\":\"urn:li:corpuser:f\"}");
      ids.add("e" + i);
    }
    refresh(backup);
    AtomicInteger copies = new AtomicInteger();
    SearchClientShim<?> client =
        clientWith(
            (request, real) -> {
              if (request.getEndpoint().startsWith("/_reindex") && copies.incrementAndGet() == 1) {
                // Slow the copy so it still runs when the retry looks for it, then lose its answer.
                String body =
                    new String(
                        request.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
                request.setJsonEntity(
                    body.replace("\"source\":{\"index\"", "\"source\":{\"size\":1,\"index\""));
                request.addParameter("requests_per_second", "4");
                real.call();
                throw new IOException("injected: the copy started but its answer was lost");
              }
              return real.call();
            });
    CreateUsageEventIndicesStep step = newStep(prefix, client);

    assertEquals(run(step), DataHubUpgradeState.FAILED);
    assertEquals(run(step), DataHubUpgradeState.SUCCEEDED);

    assertEquals(copies.get(), 1);
    refresh(index);
    assertEquals(searchIds(index, "{\"size\":30}"), ids);
    assertEquals(legacyBackups(prefix), List.of());
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testAUsageEventWrittenWhileTheIndexIsGoneCreatesTheDataStream() throws Exception {
    if (!isDataStream()) {
      throw new SkipException("Only the Elasticsearch move deletes before it creates the layout");
    }
    String prefix = "gap_";
    String index = prefix + "datahub_usage_event";
    BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components =
        components(prefix, searchClient);
    UsageEventIndexUtils.createIlmPolicy(
        OP_CONTEXT, components, prefix + "datahub_usage_event_policy");
    UsageEventIndexUtils.createIndexTemplate(
        OP_CONTEXT,
        components,
        prefix + "datahub_usage_event_index_template",
        prefix + "datahub_usage_event_policy",
        1,
        0,
        prefix);
    // What GMS sends while the migration has deleted the legacy index and not yet created the
    // data stream: a bulk create, as ElasticsearchConnector writes usage events.
    request(
        "POST",
        "/_bulk?refresh=true",
        "{\"create\":{\"_index\":\""
            + index
            + "\",\"_id\":\"g1\"}}\n"
            + "{\"type\":\"LogInEvent\",\"timestamp\":1756000010000,\"@timestamp\":1756000010000,"
            + "\"actorUrn\":\"urn:li:corpuser:g\"}\n");
    assertTrue(
        names(request("GET", "/_resolve/index/" + index, null).path("data_streams"))
            .contains(index));
    assertTrue(typeMappings(index).stream().allMatch("keyword"::equals));
    String backup = prefix + "legacy_datahub_usage_event_1";
    index(
        backup,
        "b1",
        "{\"type\":\"SearchEvent\",\"timestamp\":1756000011000,\"@timestamp\":1756000011000,"
            + "\"actorUrn\":\"urn:li:corpuser:h\"}");
    refresh(backup);

    assertEquals(runStep(prefix), DataHubUpgradeState.SUCCEEDED);

    refresh(index);
    assertEquals(searchIds(index, "{\"size\":10}"), Set.of("g1", "b1"));
    assertEquals(legacyBackups(prefix), List.of());
  }

  /** The legacy index is replaced by the managed layout holding the events, with no backup left. */
  private void assertMigrated(String prefix, Set<String> ids) throws IOException {
    String index = prefix + "datahub_usage_event";
    JsonNode resolved = request("GET", "/_resolve/index/" + index, null);
    assertEquals(resolved.path("indices").size(), 0);
    assertTrue(names(resolved.path(managedLayoutField())).contains(index));
    refresh(index);
    assertEquals(searchIds(index, "{\"size\":10}"), ids);
    assertEquals(legacyBackups(prefix), List.of());
  }

  private void indexLegacyEvents(String index) throws IOException {
    index(
        index,
        "1",
        "{\"type\":\"SearchEvent\",\"timestamp\":1756000000000,\"@timestamp\":1756000000000,"
            + "\"actorUrn\":\"urn:li:corpuser:a\"}");
    index(
        index,
        "2",
        "{\"type\":\"PageViewEvent\",\"timestamp\":1756000001000,\"@timestamp\":1756000001000,"
            + "\"actorUrn\":\"urn:li:corpuser:b\"}");
    refresh(index);
  }

  private void createLease(String prefix, String owner) throws IOException {
    request(
        "PUT",
        "/" + prefix + "legacy_datahub_usage_event_lease",
        "{\"mappings\":{\"_meta\":{\"datahub_lease_owner\":\"" + owner + "\"}}}");
  }

  private String leaseOwner(String prefix) throws IOException {
    String lease = prefix + "legacy_datahub_usage_event_lease";
    return request("GET", "/" + lease + "/_mapping", null)
        .path(lease)
        .path("mappings")
        .path("_meta")
        .path("datahub_lease_owner")
        .asText();
  }

  @FunctionalInterface
  private interface Interceptor {
    Object handle(Request request, Callable<Object> real) throws Exception;
  }

  /** The real client, except for the requests the interceptor answers or fails itself. */
  private SearchClientShim<?> clientWith(Interceptor interceptor) throws IOException {
    SearchClientShim<?> spy = Mockito.spy(searchClient);
    Mockito.doAnswer(
            invocation ->
                interceptor.handle(
                    invocation.getArgument(1),
                    () -> {
                      try {
                        return invocation.callRealMethod();
                      } catch (Throwable t) {
                        throw t instanceof Exception ? (Exception) t : new RuntimeException(t);
                      }
                    }))
        .when(spy)
        .performLowLevelRequest(Mockito.any(), Mockito.any());
    return spy;
  }

  private void recordCopy(String backup, String task, String writeIndex) throws IOException {
    request(
        "PUT",
        "/" + backup + "/_mapping",
        String.format(
            "{\"_meta\":{\"datahub_backfill_task\":\"%s\",\"datahub_backfill_write_index\":\"%s\"}}",
            task, writeIndex));
  }

  private String writeIndex(String index) throws IOException {
    if (!isDataStream()) {
      return index + "-000001";
    }
    JsonNode backing =
        request("GET", "/_data_stream/" + index, null).path("data_streams").path(0).path("indices");
    return backing.path(backing.size() - 1).path("index_name").asText();
  }

  private DataHubUpgradeState runStep(String prefix) {
    return run(newStep(prefix, searchClient));
  }

  private CreateUsageEventIndicesStep newStep(String prefix, SearchClientShim<?> client) {
    return new CreateUsageEventIndicesStep(
        components(prefix, client), Mockito.mock(ConfigurationProvider.class));
  }

  private static DataHubUpgradeState run(CreateUsageEventIndicesStep step) {
    UpgradeContext context = Mockito.mock(UpgradeContext.class);
    Mockito.when(context.opContext()).thenReturn(OP_CONTEXT);
    return step.executable().apply(context).result();
  }

  private static BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components(
      String prefix, SearchClientShim<?> client) {
    IndexConfiguration indexConfig = Mockito.mock(IndexConfiguration.class);
    Mockito.when(indexConfig.getFinalPrefix()).thenReturn(prefix);
    Mockito.when(indexConfig.getNumShards()).thenReturn(1);
    Mockito.when(indexConfig.getNumReplicas()).thenReturn(0);
    ElasticSearchConfiguration config = Mockito.mock(ElasticSearchConfiguration.class);
    Mockito.when(config.getIndex()).thenReturn(indexConfig);
    BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components =
        Mockito.mock(BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class);
    Mockito.doReturn(client).when(components).getSearchClient();
    Mockito.when(components.getConfig()).thenReturn(config);
    return components;
  }

  private void index(String index, String id, String document) throws IOException {
    request("PUT", "/" + index + "/_create/" + id, document);
  }

  private void refresh(String index) throws IOException {
    request("POST", "/" + index + "/_refresh", null);
  }

  private List<String> typeMappings(String index) throws IOException {
    List<String> types = new ArrayList<>();
    request("GET", "/" + index + "/_mapping/field/type", null)
        .forEach(
            mapping ->
                types.add(
                    mapping
                        .path("mappings")
                        .path("type")
                        .path("mapping")
                        .path("type")
                        .path("type")
                        .asText()));
    return types;
  }

  private boolean isDataStream() {
    return "data_streams".equals(managedLayoutField());
  }

  private String recordedBackfillTask(String backup) throws IOException {
    return request("GET", "/" + backup + "/_mapping", null)
        .path(backup)
        .path("mappings")
        .path("_meta")
        .path("datahub_backfill_task")
        .asText();
  }

  private List<String> legacyBackups(String prefix) throws IOException {
    return names(
        request("GET", "/_resolve/index/" + prefix + "legacy_datahub_usage_event_*", null)
            .path("indices"));
  }

  private Set<String> searchIds(String index, String body) throws IOException {
    Set<String> ids = new HashSet<>();
    request("POST", "/" + index + "/_search", body)
        .path("hits")
        .path("hits")
        .forEach(hit -> ids.add(hit.path("_id").asText()));
    return ids;
  }

  private static List<String> names(JsonNode entries) {
    List<String> names = new ArrayList<>();
    entries.forEach(entry -> names.add(entry.path("name").asText()));
    return names;
  }

  private JsonNode request(String method, String endpoint, @Nullable String body)
      throws IOException {
    Request request = new Request(method, endpoint);
    if (body != null) {
      request.setJsonEntity(body);
    }
    return OBJECT_MAPPER.readTree(
        searchClient.performLowLevelRequest(OP_CONTEXT, request).getEntity().getContent());
  }
}
