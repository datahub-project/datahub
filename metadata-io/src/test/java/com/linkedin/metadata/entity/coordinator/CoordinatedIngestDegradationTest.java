package com.linkedin.metadata.entity.coordinator;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.LOGICAL_PARENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.Status;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.AspectGenerationUtils;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.patch.builder.GlobalTagsPatchBuilder;
import com.linkedin.metadata.config.CoordinatedIngestConfiguration;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.EntityServiceAspectRetriever;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.ebean.SqlRow;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Degradation check for the coordinated-ingest path when Hazelcast is absent. The coordinator is
 * wired with a {@code null} HazelcastInstance, which the design mandates degrade to a no-op
 * distributed lock while the DB single-sorted commit stays authoritative. The Hazelcast-down batch
 * must ingest without error and produce byte-identical state to the legacy path, proving the
 * degradation preserves correctness.
 */
public class CoordinatedIngestDegradationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_schema.events,PROD)";
  private static final String LOGICAL_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_schema.events_logical,PROD)";
  private static final List<String> FIELD_PATHS = List.of("col_a", "col_b", "col_c");

  private final List<Database> databases = new ArrayList<>();

  @Test
  public void hazelcastAbsentDegradesToLegacyResult() throws Exception {
    Fixture legacy = buildFixture("degradation_legacy", false);
    Fixture degraded = buildFixture("degradation_hazelcast_down", true);

    ingestBatch(legacy);
    // Must not throw: with a null HazelcastInstance the coordinator commits directly.
    ingestBatch(degraded);

    Map<String, String> legacyState = dumpAspectState(legacy.server);
    Map<String, String> degradedState = dumpAspectState(degraded.server);

    assertFalse(degradedState.isEmpty(), "Hazelcast-down path produced no aspects");
    // Degradation is behavior-preserving: identical aspects, values, and SystemMetadata versions.
    assertEquals(degradedState, legacyState);
    assertTrue(
        containsAspect(degradedState, DATASET_URN, "datasetKey"),
        "Expected default-aspect closure to run even with Hazelcast absent");

    // The patch still materialized to an upsert under the no-op-lock commit.
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);
    EnvelopedAspect globalTags =
        degraded.service.getLatestEnvelopedAspect(
            degraded.opContext, DATASET_ENTITY_NAME, datasetUrn, GLOBAL_TAGS_ASPECT_NAME);
    assertNotNull(globalTags, "Expected the patched globalTags aspect to exist");
    assertEquals(globalTags.getSystemMetadata().getVersion(), "1");
  }

  // ---------------------------------------------------------------------------------------------
  // Fixture wiring
  // ---------------------------------------------------------------------------------------------

  private static final class Fixture {
    final EntityServiceImpl service;
    final OperationContext opContext;
    final Database server;

    Fixture(EntityServiceImpl service, OperationContext opContext, Database server) {
      this.service = service;
      this.opContext = opContext;
      this.server = server;
    }
  }

  private Fixture buildFixture(String dbName, boolean coordinatedWithoutHazelcast) {
    Database server = EbeanTestUtils.createTestServer(dbName);
    databases.add(server);

    EbeanAspectDao aspectDao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(server),
            EbeanConfiguration.testDefault,
            null,
            List.of(),
            null);

    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    EntityServiceImpl service =
        new EntityServiceImpl(
            aspectDao, mock(EventProducer.class), false, false, preProcessHooks, true);
    service.setUpdateIndicesService(mock(UpdateIndicesService.class));

    OperationContext opContext = buildOperationContext(service);

    if (coordinatedWithoutHazelcast) {
      // Null lock provider -> no-op distributed lock, DB commit remains authoritative.
      MutationCoordinator coordinator =
          new MutationCoordinator(null, testConfig(), null /* metricUtils */);
      service.setCoordinatedIngest(coordinator, new ConflictKeyResolver(), true, 0);
    }

    return new Fixture(service, opContext, server);
  }

  private static OperationContext buildOperationContext(EntityServiceImpl service) {
    EntityRegistry registry = TestOperationContexts.defaultEntityRegistry();
    return TestOperationContexts.systemContext(
        null,
        null,
        null,
        () -> registry,
        () ->
            RetrieverContext.builder()
                .aspectRetriever(
                    EntityServiceAspectRetriever.builder()
                        .entityService(service)
                        .entityRegistry(registry)
                        .build())
                .cachingAspectRetriever(
                    TestOperationContexts.emptyActiveUsersAspectRetriever(() -> registry))
                .graphRetriever(GraphRetriever.EMPTY)
                .searchRetriever(SearchRetriever.EMPTY)
                .build(),
        null,
        opContext ->
            ((EntityServiceAspectRetriever) opContext.getAspectRetriever())
                .setSystemOperationContext(opContext),
        null);
  }

  private static CoordinatedIngestConfiguration testConfig() {
    // (maxPlanExpansions, maxMutationCount, lockLeaseSeconds, lockAcquireTimeoutSeconds)
    return CoordinatedIngestConfiguration.builder()
        .maxPlanExpansions(2)
        .maxMutationCount(1000)
        .lockLeaseSeconds(30L)
        .lockAcquireTimeoutSeconds(2L)
        .lockProvider("hazelcast")
        .build();
  }

  // ---------------------------------------------------------------------------------------------
  // Batch construction and ingest
  // ---------------------------------------------------------------------------------------------

  private void ingestBatch(Fixture fixture) throws Exception {
    AspectsBatchImpl batch =
        AspectsBatchImpl.builder()
            .retrieverContext(fixture.opContext.getRetrieverContext())
            .items(buildBatchItems(fixture.opContext.getEntityRegistry()))
            .build(fixture.opContext);
    fixture.service.ingestAspects(fixture.opContext, batch, false, true);
  }

  private static List<BatchItem> buildBatchItems(EntityRegistry registry) throws Exception {
    AspectRetriever aspectRetriever =
        TestOperationContexts.emptyActiveUsersAspectRetriever(() -> registry);
    AuditStamp auditStamp = AspectGenerationUtils.createAuditStamp();
    SystemMetadata systemMetadata = AspectGenerationUtils.createSystemMetadata();

    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);
    Urn logicalDatasetUrn = UrnUtils.getUrn(LOGICAL_DATASET_URN);

    List<BatchItem> items = new ArrayList<>();

    items.add(
        ChangeItemImpl.builder()
            .urn(datasetUrn)
            .aspectName(STATUS_ASPECT_NAME)
            .recordTemplate(new Status().setRemoved(false))
            .systemMetadata(systemMetadata.copy())
            .auditStamp(auditStamp)
            .build(aspectRetriever));

    items.add(
        PatchItemImpl.builder()
            .urn(datasetUrn)
            .entitySpec(registry.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectName(GLOBAL_TAGS_ASPECT_NAME)
            .aspectSpec(
                registry.getEntitySpec(DATASET_ENTITY_NAME).getAspectSpec(GLOBAL_TAGS_ASPECT_NAME))
            .patch(
                new GlobalTagsPatchBuilder()
                    .addTag(TagUrn.createFromString("urn:li:tag:pii"), "unit-test")
                    .getJsonPatch())
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(registry));

    for (String fieldPath : FIELD_PATHS) {
      Urn fieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(datasetUrn, fieldPath);
      Urn logicalFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(logicalDatasetUrn, fieldPath);

      items.add(
          ChangeItemImpl.builder()
              .urn(fieldUrn)
              .aspectName(STATUS_ASPECT_NAME)
              .recordTemplate(new Status().setRemoved(false))
              .systemMetadata(systemMetadata.copy())
              .auditStamp(auditStamp)
              .build(aspectRetriever));

      items.add(
          ChangeItemImpl.builder()
              .urn(fieldUrn)
              .aspectName(LOGICAL_PARENT_ASPECT_NAME)
              .recordTemplate(
                  new LogicalParent().setParent(new Edge().setDestinationUrn(logicalFieldUrn)))
              .systemMetadata(systemMetadata.copy())
              .auditStamp(auditStamp)
              .build(aspectRetriever));
    }

    return items;
  }

  // ---------------------------------------------------------------------------------------------
  // State capture / comparison
  // ---------------------------------------------------------------------------------------------

  private static Map<String, String> dumpAspectState(Database server) {
    Map<String, String> state = new TreeMap<>();
    List<SqlRow> rows =
        server
            .sqlQuery(
                "select urn, aspect, version, metadata, systemmetadata from metadata_aspect_v2")
            .findList();
    for (SqlRow row : rows) {
      String key =
          row.getString("urn") + "|" + row.getString("aspect") + "|" + row.getLong("version");
      String value =
          row.getString("metadata")
              + "sysVersion="
              + systemMetadataVersion(row.getString("systemmetadata"));
      state.put(key, value);
    }
    return state;
  }

  private static String systemMetadataVersion(String systemMetadataJson) {
    if (systemMetadataJson == null) {
      return null;
    }
    try {
      JsonNode version = MAPPER.readTree(systemMetadataJson).get("version");
      return version == null || version.isNull() ? null : version.asText();
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse systemMetadata: " + systemMetadataJson, e);
    }
  }

  private static boolean containsAspect(Map<String, String> state, String urn, String aspectName) {
    String prefix = urn + "|" + aspectName + "|";
    return state.keySet().stream().anyMatch(k -> k.startsWith(prefix));
  }

  @AfterMethod(alwaysRun = true)
  public void cleanup() {
    databases.forEach(EbeanTestUtils::shutdownDatabase);
    databases.clear();
  }
}
