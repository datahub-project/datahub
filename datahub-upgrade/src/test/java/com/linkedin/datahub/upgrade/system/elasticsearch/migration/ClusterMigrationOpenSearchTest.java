package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.config.search.CopySettings;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ClusterMigrationState;
import com.linkedin.metadata.search.elasticsearch.update.MigrationIndexNames;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.xcontent.XContentType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * The operator sequence end to end, with the families served from one cluster and migrated to
 * another whose index prefix differs: prepare, mirrored live writes, the resumable copy with its
 * windowed pass, delete replay, staging adoption and verification, then the flip with the reverse
 * mirror and the usage sweep, then the rollback that points the families back without a second
 * copy. Runs against OpenSearch 2 and 3 containers by default, or against the clusters named in
 * {@code CLUSTER_MIGRATION_TEST_*} (see {@link ClusterMigrationHarness}). Opt-in: see {@link
 * ClusterMigrationHarness#requireOptIn()}.
 *
 * <p>The seed covers all four families, with four timeseries indices of different document shapes
 * (profiles with field samples, usage statistics, operations, assertion runs). {@code
 * CLUSTER_MIGRATION_TEST_SCALE} multiplies every count; the copy scenario prints per-family copy
 * throughput so runs with different copy modes or settings can be compared.
 */
public class ClusterMigrationOpenSearchTest {
  private static final long BASE = 1_700_000_000_000L;
  private static final List<SearchComponent> FAMILIES =
      List.of(
          SearchComponent.GRAPH,
          SearchComponent.SYSTEM_METADATA,
          SearchComponent.TIMESERIES,
          SearchComponent.USAGE);

  private final int scale = ClusterMigrationHarness.scale();
  private final int graphEdges = 120 * scale;
  private final int sysmetaDocs = 50 * scale;
  private final int timeseriesDocs = 100 * scale;
  private final int usageBeforeRollover = 30 * scale;
  private final int usageAfterRollover = 20 * scale;

  private ClusterMigrationHarness migrate;
  private ClusterMigrationHarness flipped;
  private ClusterMigrationHarness rolledBack;
  private String graphSource;
  private String graphTarget;
  private String sysmetaSource;
  private String sysmetaTarget;
  private String timeseriesSource;
  private String timeseriesTarget;
  private String usageStatsSource;
  private String usageStatsTarget;
  private String operationSource;
  private String operationTarget;
  private String assertionRunSource;
  private String assertionRunTarget;
  private String usageSourceAlias;
  private String usageTargetAlias;
  private String usageLiveStaging;
  private String usageSourceLiveStaging;
  private String graphLog;
  private long lostUpdateTime;
  private Long savedWindowRepeatPauseMs;

  @BeforeClass
  public void startClusters() throws Exception {
    ClusterMigrationHarness.requireOptIn();
    // A pass that still writes sleeps this long before the next round. Zero keeps a comparison
    // that never matches from burning the pause on every one of its rounds.
    savedWindowRepeatPauseMs = ClusterMigrationCopyStep.windowRepeatPauseMs;
    ClusterMigrationCopyStep.windowRepeatPauseMs = 0;
    migrate = ClusterMigrationHarness.start();
    migrate.createServingIndices();
    graphSource = migrate.servingName(SearchComponent.GRAPH, "graph_service_v1");
    graphTarget = migrate.targetName(graphSource);
    sysmetaSource =
        migrate.servingName(SearchComponent.SYSTEM_METADATA, "system_metadata_service_v1");
    sysmetaTarget = migrate.targetName(sysmetaSource);
    timeseriesSource = migrate.timeseriesIndex("dataset", "datasetProfile");
    timeseriesTarget = migrate.targetName(timeseriesSource);
    usageStatsSource = migrate.timeseriesIndex("dataset", "datasetUsageStatistics");
    usageStatsTarget = migrate.targetName(usageStatsSource);
    operationSource = migrate.timeseriesIndex("dataset", "operation");
    operationTarget = migrate.targetName(operationSource);
    assertionRunSource = migrate.timeseriesIndex("assertion", "assertionRunEvent");
    assertionRunTarget = migrate.targetName(assertionRunSource);
    usageSourceAlias = ClusterMigrationHarness.usageAlias(migrate.sourcePrefix);
    usageTargetAlias = ClusterMigrationHarness.usageAlias(migrate.targetPrefix);
    usageLiveStaging = migrate.targetPrefix + "_" + MigrationIndexNames.USAGE_LIVE_STAGING;
    usageSourceLiveStaging = migrate.sourcePrefix + "_" + MigrationIndexNames.USAGE_LIVE_STAGING;
    graphLog = migrate.targetPrefix + "_cluster_migration_log_graph";
    seedSource();
  }

  @AfterClass(alwaysRun = true)
  public void stopClusters() throws Exception {
    if (savedWindowRepeatPauseMs != null) {
      ClusterMigrationCopyStep.windowRepeatPauseMs = savedWindowRepeatPauseMs;
    }
    if (rolledBack != null) {
      rolledBack.close();
    }
    if (flipped != null) {
      flipped.close();
    }
    if (migrate != null) {
      migrate.close();
    }
  }

  private void seedSource() throws Exception {
    ClusterHttp source = migrate.source();
    seed(
        source,
        graphSource,
        ClusterMigrationOpenSearchTest::graphId,
        0,
        graphEdges,
        ClusterMigrationOpenSearchTest::edge);
    seed(
        source,
        sysmetaSource,
        i -> "sm-" + i,
        0,
        sysmetaDocs,
        ClusterMigrationOpenSearchTest::systemMetadata);
    seed(
        source,
        timeseriesSource,
        i -> "ts-" + i,
        0,
        timeseriesDocs,
        ClusterMigrationOpenSearchTest::profile);
    seed(
        source,
        usageStatsSource,
        i -> "us-" + i,
        0,
        timeseriesDocs,
        ClusterMigrationOpenSearchTest::usageStatistics);
    seed(
        source,
        operationSource,
        i -> "op-" + i,
        0,
        timeseriesDocs,
        ClusterMigrationOpenSearchTest::operation);
    seed(
        source,
        assertionRunSource,
        i -> "ar-" + i,
        0,
        timeseriesDocs,
        ClusterMigrationOpenSearchTest::assertionRunEvent);
    seed(
        source,
        usageSourceAlias,
        i -> "ev-" + i,
        0,
        usageBeforeRollover,
        ClusterMigrationOpenSearchTest::usageEvent);
    source.json("POST", "/" + usageSourceAlias + "/_rollover", null);
    seed(
        source,
        usageSourceAlias,
        i -> "ev-" + i,
        usageBeforeRollover,
        usageBeforeRollover + usageAfterRollover,
        ClusterMigrationOpenSearchTest::usageEvent);
  }

  /**
   * Seeds documents {@code id(i)} with body {@code source(i)} for {@code i} in {@code [from, to)}.
   */
  private static void seed(
      ClusterHttp cluster,
      String index,
      IntFunction<String> id,
      int from,
      int to,
      IntFunction<String> source)
      throws IOException {
    ClusterMigrationHarness.bulkCreate(
        cluster,
        index,
        IntStream.range(from, to)
            .mapToObj(i -> new ClusterMigrationHarness.Doc(id.apply(i), source.apply(i))));
  }

  /**
   * Edge ids shaped like the graph service's: Base64 of a hash, so they carry '/', '+' and '=' and
   * every path that names a document, the delete replay above all, is exercised with real ids.
   */
  private static String graphId(int i) {
    String raw =
        "urn:li:dataset:(urn:li:dataPlatform:x,a"
            + i
            + ",PROD)--"
            + (i % 2 == 0 ? "DownstreamOf" : "OwnedBy")
            + "--urn:li:dataset:(urn:li:dataPlatform:x,b"
            + i
            + ",PROD)";
    try {
      return Base64.getEncoder()
          .encodeToString(
              MessageDigest.getInstance("MD5").digest(raw.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Rewrites one numeric field so a mirrored document falls inside the copy's window. */
  private static String stamp(String json, String field, long from, long to) {
    String needle = "\"" + field + "\":" + from;
    String stamped = json.replace(needle, "\"" + field + "\":" + to);
    if (stamped.equals(json)) {
      throw new IllegalArgumentException(field + " was not " + from);
    }
    return stamped;
  }

  private static String edge(int i) {
    return "{\"source\":{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:x,a"
        + i
        + ",PROD)\",\"entityType\":\"dataset\"},\"destination\":{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:x,b"
        + i
        + ",PROD)\",\"entityType\":\"dataset\"},\"relationshipType\":\""
        + (i % 2 == 0 ? "DownstreamOf" : "OwnedBy")
        + "\",\"createdOn\":"
        + (BASE + i)
        + ",\"updatedOn\":"
        + (BASE + i)
        + "}";
  }

  private static String systemMetadata(int i) {
    return "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:x,s"
        + i
        + ",PROD)\",\"aspect\":\""
        + (i % 2 == 0 ? "status" : "ownership")
        + "\",\"runId\":\"run-1\",\"lastUpdated\":"
        + (BASE + i)
        + "}";
  }

  /** A dataset profile with eight field profiles carrying sample values: a few KB each. */
  private static String profile(int i) {
    StringBuilder fields = new StringBuilder();
    for (int f = 0; f < 8; f++) {
      if (f > 0) {
        fields.append(',');
      }
      fields
          .append("{\"fieldPath\":\"col_")
          .append(f)
          .append("\",\"uniqueCount\":")
          .append((i * 7 + f) % 1000)
          .append(",\"nullCount\":")
          .append((i + f) % 50)
          .append(",\"min\":\"")
          .append(i)
          .append("\",\"max\":\"")
          .append(i + 1000)
          .append("\",\"sampleValues\":[");
      for (int s = 0; s < 5; s++) {
        if (s > 0) {
          fields.append(',');
        }
        fields
            .append("\"sample-")
            .append(i)
            .append('-')
            .append(f)
            .append('-')
            .append(Long.toHexString(((long) i * 31 + f) * 131 + s))
            .append('"');
      }
      fields.append("]}");
    }
    return "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:x,p"
        + i
        + ",PROD)\",\"@timestamp\":"
        + (BASE + i)
        + ",\"timestampMillis\":"
        + (BASE + i)
        + ",\"isExploded\":false,\"rowCount\":"
        + (i * 1000L)
        + ",\"columnCount\":8,\"sizeInBytes\":"
        + (i * 65536L)
        + ",\"event\":{\"rowCount\":"
        + (i * 1000L)
        + ",\"columnCount\":8,\"sizeInBytes\":"
        + (i * 65536L)
        + ",\"fieldProfiles\":["
        + fields
        + "]}}";
  }

  /** Daily usage statistics with top queries and per-user and per-field counts. */
  private static String usageStatistics(int i) {
    StringBuilder users = new StringBuilder();
    for (int u = 0; u < 10; u++) {
      if (u > 0) {
        users.append(',');
      }
      users
          .append("{\"user\":\"urn:li:corpuser:user")
          .append((i + u) % 500)
          .append("\",\"count\":")
          .append((i * 3 + u) % 97)
          .append(",\"userEmail\":\"user")
          .append((i + u) % 500)
          .append("@example.com\"}");
    }
    StringBuilder fields = new StringBuilder();
    for (int f = 0; f < 10; f++) {
      if (f > 0) {
        fields.append(',');
      }
      fields
          .append("{\"fieldPath\":\"col_")
          .append(f)
          .append("\",\"count\":")
          .append((i + f) % 1000)
          .append('}');
    }
    String queries =
        "[\"SELECT col_0, col_1 FROM t"
            + i
            + " WHERE col_2 > "
            + i
            + "\",\"SELECT count(*) FROM t"
            + i
            + "\",\"SELECT * FROM t"
            + i
            + " LIMIT 100\"]";
    return "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:x,u"
        + (i % 2000)
        + ",PROD)\",\"@timestamp\":"
        + (BASE + i)
        + ",\"timestampMillis\":"
        + (BASE + i)
        + ",\"isExploded\":false,\"uniqueUserCount\":10,\"totalSqlQueries\":"
        + (i % 1000)
        + ",\"topSqlQueries\":"
        + queries
        + ",\"event\":{\"uniqueUserCount\":10,\"totalSqlQueries\":"
        + (i % 1000)
        + ",\"topSqlQueries\":"
        + queries
        + ",\"userCounts\":["
        + users
        + "],\"fieldCounts\":["
        + fields
        + "]}}";
  }

  /** A small operation event, the shape that dominates busy warehouses. */
  private static String operation(int i) {
    String type = i % 3 == 0 ? "INSERT" : i % 3 == 1 ? "UPDATE" : "DELETE";
    return "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:x,o"
        + (i % 2000)
        + ",PROD)\",\"@timestamp\":"
        + (BASE + i)
        + ",\"timestampMillis\":"
        + (BASE + i)
        + ",\"isExploded\":false,\"actor\":\"urn:li:corpuser:etl\",\"operationType\":\""
        + type
        + "\",\"numAffectedRows\":"
        + (i * 17L % 100000)
        + ",\"lastUpdatedTimestamp\":"
        + (BASE + i)
        + ",\"event\":{\"actor\":\"urn:li:corpuser:etl\",\"operationType\":\""
        + type
        + "\",\"numAffectedRows\":"
        + (i * 17L % 100000)
        + ",\"lastUpdatedTimestamp\":"
        + (BASE + i)
        + ",\"customProperties\":{\"job\":\"nightly-"
        + (i % 10)
        + "\",\"warehouse\":\"wh-"
        + (i % 4)
        + "\"}}}";
  }

  /** An assertion run with its result and native details. */
  private static String assertionRunEvent(int i) {
    String assertion = "urn:li:assertion:a" + (i % 300);
    String assertee = "urn:li:dataset:(urn:li:dataPlatform:x,d" + (i % 300) + ",PROD)";
    String result = i % 5 == 0 ? "FAILURE" : "SUCCESS";
    return "{\"urn\":\""
        + assertion
        + "\",\"@timestamp\":"
        + (BASE + i)
        + ",\"timestampMillis\":"
        + (BASE + i)
        + ",\"isExploded\":false,\"runId\":\"run-"
        + (i % 100)
        + "\",\"assertionUrn\":\""
        + assertion
        + "\",\"asserteeUrn\":\""
        + assertee
        + "\",\"status\":\"COMPLETE\",\"event\":{\"timestampMillis\":"
        + (BASE + i)
        + ",\"runId\":\"run-"
        + (i % 100)
        + "\",\"assertionUrn\":\""
        + assertion
        + "\",\"asserteeUrn\":\""
        + assertee
        + "\",\"status\":\"COMPLETE\",\"result\":{\"type\":\""
        + result
        + "\",\"actualAggValue\":"
        + (i % 1000)
        + ",\"nativeResults\":{\"rows_checked\":\""
        + (i * 100L)
        + "\",\"query\":\"SELECT count(*) FROM d"
        + (i % 300)
        + "\"}}}}";
  }

  /**
   * Search text with one character inside Latin-1 and one beyond it, so a copy that encodes its
   * requests in anything but UTF-8 is caught: the first arrives as invalid bytes, the second as
   * '?'.
   */
  private static String usageQuery(int i) {
    return "requête n°" + i + " 日本語";
  }

  private static String usageEvent(int i) {
    return "{\"@timestamp\":"
        + (BASE + i)
        + ",\"timestamp\":"
        + (BASE + i)
        + ",\"type\":\"SearchEvent\",\"query\":\""
        + usageQuery(i)
        + "\",\"actorUrn\":\"urn:li:corpuser:u\"}";
  }

  @Test
  public void prepareCreatesTargetIndicesAndMigrationRows() throws Exception {
    Map<String, DataHubUpgradeState> results = migrate.run(migrate.prepare);

    assertEquals(results.size(), 4);
    results.values().forEach(r -> assertEquals(r, DataHubUpgradeState.SUCCEEDED));
    ClusterHttp target = migrate.target();
    assertTrue(target.indexExists(graphTarget), "graph index on the target");
    assertTrue(target.indexExists(sysmetaTarget), "system metadata index on the target");
    for (String index :
        List.of(timeseriesTarget, usageStatsTarget, operationTarget, assertionRunTarget)) {
      assertTrue(target.indexExists(index), "timeseries index on the target: " + index);
    }
    assertEquals(
        ClusterMigrationHarness.names(target.aliasMembers(usageTargetAlias)),
        List.of(usageTargetAlias + "-000001"));
    assertTrue(target.indexExists(usageLiveStaging), "live usage staging index on the target");
    assertTrue(target.indexExists(graphLog));
    // Nothing was copied yet and nothing is served from the target.
    assertEquals(target.count(graphTarget, null), 0);
    ClusterMigrationRow row = migrate.row(SearchComponent.GRAPH, "primary", "secondary");
    assertTrue(row.existed());
    assertEquals(row.status().orElseThrow(), ClusterMigrationState.Status.PREPARED);
    assertTrue(row.getLong(ClusterMigrationState.MIRROR_START_TIME).isPresent());
  }

  @Test(dependsOnMethods = "prepareCreatesTargetIndicesAndMigrationRows")
  public void liveWritesAreMirroredUnderTheTargetNames() throws Exception {
    for (SearchComponent family : FAMILIES) {
      migrate.markLiveWriters(family);
    }
    migrate
        .registry
        .bulkProcessorFor(SearchComponent.GRAPH)
        .add(
            migrate.opContext,
            "edge-live",
            new UpdateRequest(graphSource, "edge-live")
                .docAsUpsert(true)
                .doc(edge(graphEdges), XContentType.JSON));
    // An id with a '/' in it, as the graph service makes them: the replay must never put one in a
    // request path.
    String deletedEdge =
        IntStream.range(0, graphEdges)
            .mapToObj(ClusterMigrationOpenSearchTest::graphId)
            .filter(id -> id.contains("/"))
            .findFirst()
            .orElseThrow();
    migrate
        .registry
        .bulkProcessorFor(SearchComponent.GRAPH)
        .add(migrate.opContext, deletedEdge, new DeleteRequest(graphSource, deletedEdge));
    migrate
        .registry
        .bulkProcessorFor(SearchComponent.SYSTEM_METADATA)
        .add(
            migrate.opContext,
            "sm-live",
            new UpdateRequest(sysmetaSource, "sm-live")
                .docAsUpsert(true)
                .doc(systemMetadata(sysmetaDocs), XContentType.JSON));
    migrate
        .registry
        .bulkProcessorFor(SearchComponent.TIMESERIES)
        .add(
            migrate.opContext,
            "ts-live",
            new IndexRequest(timeseriesSource)
                .id("ts-live")
                .source(profile(timeseriesDocs), XContentType.JSON));
    migrate
        .registry
        .bulkProcessorFor(SearchComponent.USAGE)
        .add(
            migrate.opContext,
            "ev-live",
            new IndexRequest(usageSourceAlias)
                .id("ev-live")
                .opType(DocWriteRequest.OpType.CREATE)
                .source(usageEvent(usageBeforeRollover + usageAfterRollover), XContentType.JSON));
    // One document per mutable family, mirrored and then left alone, with its guard inside the
    // window. The pass can come out clean only when the target's stored source compares equal to
    // the source document the script receives.
    long now = System.currentTimeMillis();
    migrate
        .registry
        .bulkProcessorFor(SearchComponent.GRAPH)
        .add(
            migrate.opContext,
            "edge-current",
            new UpdateRequest(graphSource, "edge-current")
                .docAsUpsert(true)
                .doc(
                    stamp(edge(graphEdges + 1), "updatedOn", BASE + graphEdges + 1, now),
                    XContentType.JSON));
    migrate
        .registry
        .bulkProcessorFor(SearchComponent.SYSTEM_METADATA)
        .add(
            migrate.opContext,
            "sm-current",
            new UpdateRequest(sysmetaSource, "sm-current")
                .docAsUpsert(true)
                .doc(
                    stamp(
                        systemMetadata(sysmetaDocs + 1),
                        "lastUpdated",
                        BASE + sysmetaDocs + 1,
                        now),
                    XContentType.JSON));
    migrate
        .registry
        .bulkProcessorFor(SearchComponent.TIMESERIES)
        .add(
            migrate.opContext,
            "ts-current",
            new IndexRequest(timeseriesSource)
                .id("ts-current")
                .source(
                    stamp(
                        stamp(
                            profile(timeseriesDocs + 1),
                            "@timestamp",
                            BASE + timeseriesDocs + 1,
                            now),
                        "timestampMillis",
                        BASE + timeseriesDocs + 1,
                        now),
                    XContentType.JSON));
    migrate.flushAll();

    ClusterHttp source = migrate.source();
    ClusterHttp target = migrate.target();
    assertNotNull(target.jsonOrNull("GET", "/" + graphTarget + "/_doc/edge-live", null));
    ObjectNode deletedEdgeOnly = source.object();
    deletedEdgeOnly.putObject("ids").putArray("values").add(deletedEdge);
    assertEquals(source.count(graphSource, deletedEdgeOnly), 0, "deleted on the serving cluster");
    assertEquals(target.count(graphTarget, deletedEdgeOnly), 0, "the delete was mirrored");
    assertNotNull(target.jsonOrNull("GET", "/" + sysmetaTarget + "/_doc/sm-live", null));
    assertNotNull(target.jsonOrNull("GET", "/" + timeseriesTarget + "/_doc/ts-live", null));
    assertNotNull(target.jsonOrNull("GET", "/" + graphTarget + "/_doc/edge-current", null));
    assertNotNull(target.jsonOrNull("GET", "/" + sysmetaTarget + "/_doc/sm-current", null));
    assertNotNull(target.jsonOrNull("GET", "/" + timeseriesTarget + "/_doc/ts-current", null));
    // Usage writes go to the live staging index, never to the target's own write index.
    assertEquals(target.count(usageLiveStaging, null), 1);
    assertEquals(target.count(usageTargetAlias + "-000001", null), 0);
    assertEquals(target.count(graphLog, null), 1);
    ClusterMigrationRow row = migrate.row(SearchComponent.GRAPH, "primary", "secondary");
    assertTrue(ClusterMigrationState.liveWritersMirroringSince(row.state()).isPresent());

    // Updates the mirror lost, made on one cluster only: the graph edge is newer on the source,
    // the system metadata row newer on the target. The windowed pass must repair the first and
    // leave the second alone.
    lostUpdateTime = System.currentTimeMillis();
    ObjectNode edgeUpdate = source.object();
    edgeUpdate.putObject("doc").put("updatedOn", lostUpdateTime);
    source.json("POST", "/" + graphSource + "/_update/edge-live?refresh=true", edgeUpdate);
    ObjectNode olderMetadata = source.object();
    olderMetadata.putObject("doc").put("lastUpdated", lostUpdateTime).put("runId", "older");
    source.json("POST", "/" + sysmetaSource + "/_update/sm-live?refresh=true", olderMetadata);
    ObjectNode newerMetadata = target.object();
    newerMetadata.putObject("doc").put("lastUpdated", lostUpdateTime + 1000).put("runId", "newer");
    target.json("POST", "/" + sysmetaTarget + "/_update/sm-live?refresh=true", newerMetadata);
  }

  @Test(dependsOnMethods = "liveWritesAreMirroredUnderTheTargetNames")
  public void copyMovesEveryFamilyAndVerifies() throws Exception {
    Map<String, DataHubUpgradeState> results = migrate.run(migrate.copy);
    reportCopyTiming();
    // The copy step records the copier its partitions ran with.
    assertEquals(
        migrate
            .row(SearchComponent.GRAPH, "primary", "secondary")
            .get(ClusterMigrationState.COPY_MODE),
        copySettings().getMode());

    assertEquals(results.size(), 4, results.toString());
    results.forEach((id, r) -> assertEquals(r, DataHubUpgradeState.SUCCEEDED, id));
    ClusterHttp source = migrate.source();
    ClusterHttp target = migrate.target();
    for (SearchComponent family : FAMILIES) {
      ClusterMigrationRow row = migrate.row(family, "primary", "secondary");
      assertEquals(
          row.status().orElseThrow(), ClusterMigrationState.Status.VERIFIED, family.name());
      // Usage events are created once. Every other family's last pass is the one that found the
      // target already current, including the mirrored document whose guard is inside the window.
      if (family != SearchComponent.USAGE) {
        assertEquals(ClusterMigrationCopyStep.writtenByWindowPass(row), 0L, family.name());
      }
    }
    assertEquals(target.count(graphTarget, null), source.count(graphSource, null));
    // Seeded, minus the deleted edge, plus edge-live and edge-current.
    assertEquals(target.count(graphTarget, null), graphEdges + 1);
    assertEquals(target.count(sysmetaTarget, null), sysmetaDocs + 2);
    assertEquals(target.count(timeseriesTarget, null), timeseriesDocs + 2);
    assertEquals(target.count(usageStatsTarget, null), timeseriesDocs);
    assertEquals(target.count(operationTarget, null), timeseriesDocs);
    assertEquals(target.count(assertionRunTarget, null), timeseriesDocs);
    assertEquals(target.count(usageTargetAlias, null), source.count(usageSourceAlias, null));
    List<String> members = ClusterMigrationHarness.names(target.aliasMembers(usageTargetAlias));
    assertTrue(members.contains(usageTargetAlias + "-000001"), members.toString());
    assertTrue(
        members.contains(migrate.targetPrefix + "_datahub_usage_migration-000001"),
        members.toString());
    assertTrue(members.contains(usageLiveStaging), members.toString());
    String historicalStaging = migrate.targetPrefix + "_datahub_usage_migration-000001";
    JsonNode historical = target.json("GET", "/" + historicalStaging + "/_settings", null);
    assertNull(historical.findValue("blocks"), "adopted members are never write-blocked");
    // Text outside ASCII survives the copy unchanged.
    JsonNode copied = target.json("GET", "/" + historicalStaging + "/_doc/ev-0", null);
    assertEquals(copied.path("_source").path("query").asText(), usageQuery(0));
    // Graph was split by relationship type and then by createdOn; the plan has more than two parts.
    ClusterMigrationRow graph = migrate.row(SearchComponent.GRAPH, "primary", "secondary");
    assertTrue(graph.getLong(ClusterMigrationState.PARTITION_COUNT).orElseThrow() > 2);
    assertEquals(
        graph.get(ClusterMigrationState.VERIFY_PREFIX + graphSource + ".sampledMissing"), "0");
    // The windowed pass of a mutable family replaces what the target holds in an older version
    // and leaves what it holds in a newer one.
    JsonNode edgeLive = target.json("GET", "/" + graphTarget + "/_doc/edge-live", null);
    assertEquals(
        edgeLive.path("_source").path("updatedOn").asLong(),
        lostUpdateTime,
        "the update the mirror lost was repaired from the source");
    JsonNode smLive = target.json("GET", "/" + sysmetaTarget + "/_doc/sm-live", null);
    assertEquals(
        smLive.path("_source").path("runId").asText(),
        "newer",
        "a newer document on the target is not undone");
    // The live writers stop logging once their poll sees the row verified; the log itself goes
    // with the next system update, so nothing is deleted under a pod that still logs.
    assertTrue(target.indexExists(graphLog));
  }

  /**
   * Prints how long the partition phase of each family's copy took and its throughput, from the
   * timestamps and per-partition counts the copy step records, next to the whole step's wall-clock
   * time (which adds the window pass, delete replay, adoption and verification).
   */
  private void reportCopyTiming() {
    CopySettings copy = copySettings();
    StringBuilder out =
        new StringBuilder(
            String.format(
                "%ncopy timing: mode=%s scale=%d parallelism=%d batchSize=%d targetDocsPerPartition=%d%n",
                copy.getMode(),
                scale,
                copy.getParallelism(),
                copy.getBatchSize(),
                copy.getTargetDocsPerPartition()));
    out.append(
        String.format(
            "%-16s %12s %6s %9s %10s %9s%n",
            "family", "docs", "parts", "copy_s", "docs/s", "step_s"));
    for (SearchComponent family : FAMILIES) {
      ClusterMigrationRow row = migrate.row(family, "primary", "secondary");
      int parts = row.getLong(ClusterMigrationState.PARTITION_COUNT).orElse(0L).intValue();
      long docs = 0;
      for (int p = 0; p < parts; p++) {
        docs +=
            row.getLong(
                    ClusterMigrationState.partitionKey(p, ClusterMigrationCopyStep.PARTITION_READ))
                .orElse(0L);
      }
      long started = row.getLong(ClusterMigrationState.COPY_START_TIME).orElse(0L);
      long finished = row.getLong(ClusterMigrationState.COPY_COMPLETE_TIME).orElse(started);
      double seconds = (finished - started) / 1000.0;
      Duration step =
          migrate.stepDurations.getOrDefault(
              "ClusterMigrationCopy_" + family.getConfigKey(), Duration.ZERO);
      out.append(
          String.format(
              "%-16s %12d %6d %9.1f %10.0f %9.1f%n",
              family.getConfigKey(),
              docs,
              parts,
              seconds,
              seconds > 0 ? docs / seconds : 0d,
              step.toMillis() / 1000.0));
    }
    System.out.print(out);
  }

  private CopySettings copySettings() {
    return migrate.config.getClusterMigration().forComponent(SearchComponent.GRAPH).getCopy();
  }

  @Test(dependsOnMethods = "copyMovesEveryFamilyAndVerifies")
  public void flipServesFromTheTargetMirrorsBackAndSweepsUsage() throws Exception {
    // Events that reached the old write index between the copy and the restart, across a rollover
    // the sweep has to walk.
    seed(
        migrate.source(),
        usageSourceAlias,
        i -> "ev-inflight-" + i,
        0,
        3,
        i -> usageEvent(900 + i));
    migrate.source().json("POST", "/" + usageSourceAlias + "/_rollover", null);
    seed(
        migrate.source(),
        usageSourceAlias,
        i -> "ev-inflight-late-" + i,
        0,
        2,
        i -> usageEvent(910 + i));
    long sourceUsage = migrate.source().count(usageSourceAlias, null);

    flipped = migrate.redeployFlipped();
    Map<String, DataHubUpgradeState> prepared = flipped.run(flipped.prepare);
    prepared.forEach((id, r) -> assertEquals(r, DataHubUpgradeState.SUCCEEDED, id));
    // The pods of the flipped rollout report the reverse mirror live; the sweep waits for them.
    for (SearchComponent family : FAMILIES) {
      flipped.markLiveWriters(family);
    }
    Map<String, DataHubUpgradeState> copied = flipped.run(flipped.copy);
    copied.forEach((id, r) -> assertEquals(r, DataHubUpgradeState.SUCCEEDED, id));

    ClusterHttp target = flipped.target();
    ClusterMigrationRow forward = flipped.row(SearchComponent.USAGE, "primary", "secondary");
    assertEquals(forward.get(ClusterMigrationState.USAGE_POST_FLIP_SWEEP), "done");
    target.refresh(usageTargetAlias);
    assertEquals(target.count(usageTargetAlias, null), sourceUsage);
    JsonNode live = target.json("GET", "/" + usageLiveStaging + "/_settings", null);
    assertNull(live.findValue("blocks"), "the live staging index stays writable for a rollback");
    assertFalse(
        target.indexExists(graphLog),
        "the first system update after the flip removes the verified copy's delete log");

    // The reverse mirror: a graph write now lands on the target and is copied back to the source.
    String servingGraph = flipped.servingName(SearchComponent.GRAPH, "graph_service_v1");
    assertEquals(servingGraph, graphTarget);
    flipped
        .registry
        .bulkProcessorFor(SearchComponent.GRAPH)
        .add(
            flipped.opContext,
            "edge-after-flip",
            new UpdateRequest(servingGraph, "edge-after-flip")
                .docAsUpsert(true)
                .doc(edge(graphEdges + 1), XContentType.JSON));
    // A usage event of the bake lands in the target's write index and is mirrored into the live
    // staging index on the source, which is not part of the source alias yet.
    flipped
        .registry
        .bulkProcessorFor(SearchComponent.USAGE)
        .add(
            flipped.opContext,
            "ev-bake",
            new IndexRequest(usageTargetAlias)
                .id("ev-bake")
                .opType(DocWriteRequest.OpType.CREATE)
                .source(usageEvent(950), XContentType.JSON));
    flipped.flushAll();
    assertNotNull(target.jsonOrNull("GET", "/" + graphTarget + "/_doc/edge-after-flip", null));
    assertNotNull(
        flipped.source().jsonOrNull("GET", "/" + graphSource + "/_doc/edge-after-flip", null));
    ClusterMigrationRow reverse = flipped.row(SearchComponent.GRAPH, "secondary", "primary");
    assertEquals(reverse.status().orElseThrow(), ClusterMigrationState.Status.PREPARED);
    assertEquals(
        countIds(target, usageTargetAlias, "ev-bake"),
        1,
        "ev-bake on the target is in " + indicesHolding(target, "ev-bake"));
    assertNotNull(
        flipped.source().jsonOrNull("GET", "/" + usageSourceLiveStaging + "/_doc/ev-bake", null),
        "ev-bake on the source is in " + indicesHolding(flipped.source(), "ev-bake"));
    assertEquals(countIds(flipped.source(), usageSourceAlias, "ev-bake"), 0);
  }

  @Test(dependsOnMethods = "flipServesFromTheTargetMirrorsBackAndSweepsUsage")
  public void rollbackServesFromTheSourceAgainWithoutASecondCopy() throws Exception {
    Map<SearchComponent, Long> copyCompleted = new EnumMap<>(SearchComponent.class);
    for (SearchComponent family : FAMILIES) {
      copyCompleted.put(
          family,
          flipped
              .row(family, "primary", "secondary")
              .getLong(ClusterMigrationState.COPY_COMPLETE_TIME)
              .orElseThrow());
    }
    // Events that reached the target's write index between the last mirrored write and the restart.
    seed(
        flipped.target(),
        usageTargetAlias,
        i -> "ev-inflight-back-" + i,
        0,
        2,
        i -> usageEvent(970 + i));
    long targetUsage = flipped.target().count(usageTargetAlias, null);

    // The documented rollback: the families point back at the old cluster while the reverse
    // mirror's rows are still PREPARED. Nothing is in flight, so the pointers move back and the
    // verified forward rows are left as they are.
    rolledBack = migrate.redeployRolledBack();
    Map<String, DataHubUpgradeState> prepared = rolledBack.run(rolledBack.prepare);
    prepared.forEach((id, r) -> assertEquals(r, DataHubUpgradeState.SUCCEEDED, id));
    // The blocking job forgot the forward rows' markers; this rollout's pods report again.
    for (SearchComponent family : FAMILIES) {
      assertTrue(
          ClusterMigrationState.liveWritersMirroringSince(
                  rolledBack.row(family, "primary", "secondary").state())
              .isEmpty(),
          family + " keeps the markers of the migrating deploy's pods");
      rolledBack.markLiveWriters(family);
    }
    Map<String, DataHubUpgradeState> copied = rolledBack.run(rolledBack.copy);
    copied.forEach((id, r) -> assertEquals(r, DataHubUpgradeState.SUCCEEDED, id));

    ClusterHttp source = rolledBack.source();
    ClusterHttp target = rolledBack.target();
    for (SearchComponent family : FAMILIES) {
      ClusterMigrationRow forward = rolledBack.row(family, "primary", "secondary");
      assertEquals(forward.status().orElseThrow(), ClusterMigrationState.Status.VERIFIED);
      assertEquals(
          forward.getLong(ClusterMigrationState.COPY_COMPLETE_TIME).orElseThrow(),
          copyCompleted.get(family),
          family + " was copied again");
      assertEquals(rolledBack.activeMigration(family), forward.getUrn().getId());
    }
    assertEquals(rolledBack.servingName(SearchComponent.GRAPH, "graph_service_v1"), graphSource);

    // Everything the target's alias received during the bake is served by the source again: the
    // mirrored events through the adopted staging index, the in-flight ones through the sweep.
    source.refresh(usageSourceAlias);
    assertEquals(source.count(usageSourceAlias, null), targetUsage);
    assertEquals(countIds(source, usageSourceAlias, "ev-bake"), 1);
    JsonNode sourceLive = source.json("GET", "/" + usageSourceLiveStaging + "/_settings", null);
    assertNull(sourceLive.findValue("blocks"), "the live staging index stays writable");
    ClusterMigrationRow reverse = rolledBack.row(SearchComponent.USAGE, "secondary", "primary");
    assertEquals(reverse.get(ClusterMigrationState.USAGE_POST_FLIP_SWEEP), "done");

    // The forward mirror is live again: the target's live staging index takes writes once more,
    // and the forward row knows where the sweep of the next flip starts.
    ClusterMigrationRow forwardUsage =
        rolledBack.row(SearchComponent.USAGE, "primary", "secondary");
    assertEquals(forwardUsage.get(ClusterMigrationState.USAGE_POST_FLIP_SWEEP), "pending");
    assertEquals(
        forwardUsage.get(ClusterMigrationState.USAGE_LAST_SOURCE_WRITE_INDEX),
        IndexLayoutResolver.resolve(source, SearchComponent.USAGE, usageSourceAlias).writeIndex());
    rolledBack
        .registry
        .bulkProcessorFor(SearchComponent.USAGE)
        .add(
            rolledBack.opContext,
            "ev-after-rollback",
            new IndexRequest(usageSourceAlias)
                .id("ev-after-rollback")
                .opType(DocWriteRequest.OpType.CREATE)
                .source(usageEvent(980), XContentType.JSON));
    rolledBack
        .registry
        .bulkProcessorFor(SearchComponent.GRAPH)
        .add(
            rolledBack.opContext,
            "edge-after-rollback",
            new UpdateRequest(graphSource, "edge-after-rollback")
                .docAsUpsert(true)
                .doc(edge(graphEdges + 2), XContentType.JSON));
    rolledBack.flushAll();
    assertNotNull(
        target.jsonOrNull("GET", "/" + usageLiveStaging + "/_doc/ev-after-rollback", null));
    assertEquals(countIds(target, usageTargetAlias, "ev-after-rollback"), 1);
    assertNotNull(target.jsonOrNull("GET", "/" + graphTarget + "/_doc/edge-after-rollback", null));
    assertNotNull(source.jsonOrNull("GET", "/" + graphSource + "/_doc/edge-after-rollback", null));
  }

  private static long countIds(ClusterHttp cluster, String index, String id) throws IOException {
    ObjectNode body = cluster.object();
    body.putObject("query").putObject("ids").putArray("values").add(id);
    return cluster.json("POST", "/" + index + "/_count", body).path("count").asLong();
  }

  /** Every index on the cluster holding a document with this id, for a failure message. */
  private static List<String> indicesHolding(ClusterHttp cluster, String id) throws IOException {
    ObjectNode body = cluster.object();
    body.putObject("query").putObject("ids").putArray("values").add(id);
    body.put("_source", false);
    List<String> indices = new ArrayList<>();
    cluster
        .json("POST", "/_all/_search", body)
        .path("hits")
        .path("hits")
        .forEach(hit -> indices.add(hit.path("_index").asText()));
    return indices;
  }
}
