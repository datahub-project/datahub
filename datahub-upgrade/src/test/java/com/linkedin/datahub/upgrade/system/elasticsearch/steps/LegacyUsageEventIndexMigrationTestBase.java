package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.upgrade.UpgradeContext;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.opensearch.client.Request;
import org.testcontainers.containers.GenericContainer;
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
    IndexConfiguration indexConfig = Mockito.mock(IndexConfiguration.class);
    Mockito.when(indexConfig.getFinalPrefix()).thenReturn(prefix);
    Mockito.when(indexConfig.getNumShards()).thenReturn(1);
    Mockito.when(indexConfig.getNumReplicas()).thenReturn(0);
    ElasticSearchConfiguration config = Mockito.mock(ElasticSearchConfiguration.class);
    Mockito.when(config.getIndex()).thenReturn(indexConfig);
    BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components =
        Mockito.mock(BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class);
    Mockito.doReturn(searchClient).when(components).getSearchClient();
    Mockito.when(components.getConfig()).thenReturn(config);
    UpgradeContext context = Mockito.mock(UpgradeContext.class);
    Mockito.when(context.opContext()).thenReturn(OP_CONTEXT);

    return new CreateUsageEventIndicesStep(components, Mockito.mock(ConfigurationProvider.class))
        .executable()
        .apply(context)
        .result();
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
