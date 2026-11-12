package com.linkedin.datahub.upgrade.system.schemafield;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_KEY_ASPECT;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.schemafields.sideeffects.SchemaFieldSideEffect;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGeneratorRegistry;
import com.linkedin.metadata.timeline.eventgenerator.SchemaMetadataChangeEventGenerator;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GenerateSchemaFieldsFromSchemaMetadataStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)");
  private static final AuditStamp AUDIT_STAMP =
      new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:datahub")).setTime(0L);

  private EntityService<?> mockEntityService;
  private AspectDao mockAspectDao;

  @BeforeMethod
  public void setup() {
    mockEntityService = mock(EntityService.class);
    mockAspectDao = mock(AspectDao.class);
  }

  @Test
  public void testFingerprintAndIdEncodeFlags() {
    GenerateSchemaFieldsFromSchemaMetadataStep bothOff = newStep(false, false, false);
    assertEquals(bothOff.getFingerprint(), "d0-o0");
    assertEquals(bothOff.id(), "schema-field-from-schema-metadata-v2-d0-o0");

    GenerateSchemaFieldsFromSchemaMetadataStep bothOn = newStep(false, true, true);
    assertEquals(bothOn.getFingerprint(), "d1-o1");
    assertEquals(bothOn.id(), "schema-field-from-schema-metadata-v2-d1-o1");

    assertEquals(GenerateSchemaFieldsFromSchemaMetadataStep.fingerprint(true, false), "d1-o0");
    assertEquals(GenerateSchemaFieldsFromSchemaMetadataStep.fingerprint(false, true), "d0-o1");
  }

  @Test
  public void testGetUrnLike() {
    assertEquals(newStep(false, false, false).getUrnLike(), "urn:li:dataset:%");
  }

  @Test
  public void testSkipWhenReprocessEnabled() {
    GenerateSchemaFieldsFromSchemaMetadataStep step = newStep(true, true, true);
    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    DataHubUpgradeResult succeeded = mock(DataHubUpgradeResult.class);
    when(succeeded.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(succeeded));

    assertFalse(step.skip(mockContext));
  }

  @Test
  public void testSkipWhenFingerprintAlreadySucceeded() {
    GenerateSchemaFieldsFromSchemaMetadataStep step = newStep(false, true, false);
    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    DataHubUpgradeResult succeeded = mock(DataHubUpgradeResult.class);
    when(succeeded.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(), eq(mockEntityService)))
        .thenReturn(Optional.of(succeeded));

    assertTrue(step.skip(mockContext));
  }

  @Test
  public void testDoesNotSkipWhenNoPreviousResult() {
    GenerateSchemaFieldsFromSchemaMetadataStep step = newStep(false, false, false);
    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());

    assertFalse(step.skip(mockContext));
  }

  @Test
  public void testCycleBackToPriorFingerprintSkipsWithoutReprocess() {
    // d1 SUCCEEDED → switch to d0 (runs) → switch back to d1 skips; reprocess forces d1 again.
    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    DataHubUpgradeResult succeeded = mock(DataHubUpgradeResult.class);
    when(succeeded.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);

    GenerateSchemaFieldsFromSchemaMetadataStep d1 = newStep(false, true, false); // d1-o0
    GenerateSchemaFieldsFromSchemaMetadataStep d0 = newStep(false, false, false); // d0-o0
    Urn d1Urn = BootstrapStep.getUpgradeUrn(d1.id());
    Urn d0Urn = BootstrapStep.getUpgradeUrn(d0.id());

    // After d1 completed: first-time disable to d0 has no result → must run
    when(mockUpgrade.getUpgradeResult(eq(OP_CONTEXT), eq(d1Urn), eq(mockEntityService)))
        .thenReturn(Optional.of(succeeded));
    when(mockUpgrade.getUpgradeResult(eq(OP_CONTEXT), eq(d0Urn), eq(mockEntityService)))
        .thenReturn(Optional.empty());
    assertFalse(d0.skip(mockContext), "d1→d0 first transition must not skip");

    // After d0 also SUCCEEDED: returning to d1 looks up d1's prior SUCCEEDED → skips
    when(mockUpgrade.getUpgradeResult(eq(OP_CONTEXT), eq(d0Urn), eq(mockEntityService)))
        .thenReturn(Optional.of(succeeded));
    assertTrue(d1.skip(mockContext), "d1→d0→d1 cycle must skip without reprocess");
    assertTrue(d0.skip(mockContext), "d0 still skips once it has SUCCEEDED");

    // Manual reprocess on d1 forces a run even after the cycle
    GenerateSchemaFieldsFromSchemaMetadataStep d1Reprocess = newStep(true, true, false);
    assertFalse(d1Reprocess.skip(mockContext), "reprocess must not skip after cycle");
  }

  @Test
  public void testPartitionChunksBySize() {
    List<Integer> items = List.of(1, 2, 3, 4, 5, 6, 7);
    List<List<Integer>> chunks = GenerateSchemaFieldsFromSchemaMetadataStep.partition(items, 3);
    assertEquals(chunks.size(), 3);
    assertEquals(chunks.get(0), List.of(1, 2, 3));
    assertEquals(chunks.get(1), List.of(4, 5, 6));
    assertEquals(chunks.get(2), List.of(7));
    assertTrue(GenerateSchemaFieldsFromSchemaMetadataStep.partition(List.of(), 3).isEmpty());
  }

  @Test
  public void testToRestateMclItemsStampsRestateAndSystemUpdateSource() {
    GenerateSchemaFieldsFromSchemaMetadataStep step = newStep(false, true, true);
    SystemAspect systemAspect = mockSchemaMetadataSystemAspect(OP_CONTEXT, 2);

    List<MCLItem> restateItems = step.toRestateMclItems(List.of(systemAspect));
    assertEquals(restateItems.size(), 1);
    assertEquals(restateItems.get(0).getChangeType(), ChangeType.RESTATE);
    assertEquals(
        restateItems
            .get(0)
            .getSystemMetadata()
            .getProperties()
            .get(com.linkedin.metadata.Constants.APP_SOURCE),
        com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE);
  }

  @Test
  public void testBuildSideEffectProposalsEmitsDomainAndOwnershipMirrors() {
    int fieldCount = 2;
    Domains domains =
        new Domains().setDomains(new UrnArray(List.of(UrnUtils.getUrn("urn:li:domain:finance"))));
    Ownership ownership =
        new Ownership()
            .setOwners(
                new OwnerArray(
                    List.of(
                        new Owner()
                            .setOwner(UrnUtils.getUrn("urn:li:corpuser:jdoe"))
                            .setType(OwnershipType.TECHNICAL_OWNER))))
            .setLastModified(AUDIT_STAMP);

    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    EntityRegistry entityRegistry = spy(TestOperationContexts.defaultEntityRegistry());
    SchemaFieldSideEffect sideEffect = schemaFieldSideEffect(true, true);
    when(entityRegistry.getAllMCPSideEffects()).thenReturn(List.of(sideEffect));
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    when(mockAspectRetriever.getLatestAspectObjects(
            any(OperationFingerprint.class), eq(Set.of(DATASET_URN)), anySet()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Set<String> aspectNames = invocation.getArgument(2);
              Map<String, Aspect> aspects = new HashMap<>();
              if (aspectNames.contains(DOMAINS_ASPECT_NAME)) {
                aspects.put(DOMAINS_ASPECT_NAME, new Aspect(domains.data()));
              }
              if (aspectNames.contains(OWNERSHIP_ASPECT_NAME)) {
                aspects.put(OWNERSHIP_ASPECT_NAME, new Aspect(ownership.data()));
              }
              return Map.of(DATASET_URN, aspects);
            });

    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever);
    GenerateSchemaFieldsFromSchemaMetadataStep step =
        new GenerateSchemaFieldsFromSchemaMetadataStep(
            opContext, mockEntityService, mockAspectDao, 10, 100, 1000, false, true, true);

    List<MCLItem> mclItems =
        step.toRestateMclItems(List.of(mockSchemaMetadataSystemAspect(opContext, fieldCount)));
    List<MCPItem> proposals = step.buildSideEffectProposals(mclItems);

    List<MCPItem> domainProposals =
        proposals.stream()
            .filter(item -> DOMAINS_ASPECT_NAME.equals(item.getAspectName()))
            .collect(Collectors.toList());
    List<MCPItem> ownershipProposals =
        proposals.stream()
            .filter(item -> OWNERSHIP_ASPECT_NAME.equals(item.getAspectName()))
            .collect(Collectors.toList());

    assertEquals(domainProposals.size(), fieldCount);
    assertEquals(ownershipProposals.size(), fieldCount);
    assertEquals(
        domainProposals.stream().map(MCPItem::getUrn).collect(Collectors.toSet()),
        expectedFieldUrns(fieldCount));
    assertEquals(
        ownershipProposals.stream().map(MCPItem::getUrn).collect(Collectors.toSet()),
        expectedFieldUrns(fieldCount));
    assertEquals(
        domainProposals.stream()
            .map(item -> item.getAspect(Domains.class).getDomains())
            .collect(Collectors.toSet()),
        Set.of(domains.getDomains()));
    assertEquals(
        ownershipProposals.stream()
            .map(item -> item.getAspect(Ownership.class).getOwners())
            .collect(Collectors.toSet()),
        Set.of(ownership.getOwners()));

    verify(mockEntityService, times(0))
        .ingestAspects(any(), any(), any(Boolean.class), any(Boolean.class));
  }

  @Test
  public void testIngestSideEffectProposalsChunksByBatchSize() {
    // batchSize=5 → 12 proposals should be 3 async ingestProposal calls (5+5+2)
    GenerateSchemaFieldsFromSchemaMetadataStep step =
        new GenerateSchemaFieldsFromSchemaMetadataStep(
            OP_CONTEXT,
            mockEntityService,
            mockAspectDao,
            /* batchSize */ 5,
            /* batchDelayMs */ 0,
            1000,
            false,
            false,
            false);

    List<MCPItem> proposals = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      MCPItem item = mock(MCPItem.class);
      when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
      proposals.add(item);
    }

    step.ingestSideEffectProposals(proposals);

    ArgumentCaptor<AspectsBatch> batchCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(mockEntityService, times(3))
        .ingestProposal(eq(OP_CONTEXT), batchCaptor.capture(), eq(true));
    verify(mockEntityService, times(0))
        .ingestAspects(any(), any(), any(Boolean.class), any(Boolean.class));
    verify(mockEntityService, times(0))
        .deleteAspect(any(), anyString(), anyString(), any(), anyBoolean());

    List<AspectsBatch> batches = batchCaptor.getAllValues();
    assertEquals(batches.get(0).getItems().size(), 5);
    assertEquals(batches.get(1).getItems().size(), 5);
    assertEquals(batches.get(2).getItems().size(), 2);
  }

  @Test
  public void testIngestSideEffectProposalsDeletesSyncNotAsync() {
    GenerateSchemaFieldsFromSchemaMetadataStep step = newStep(false, true, true);
    Urn fieldUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD),col_a)");

    MCPItem deleteItem = mock(MCPItem.class);
    when(deleteItem.getChangeType()).thenReturn(ChangeType.DELETE);
    when(deleteItem.getUrn()).thenReturn(fieldUrn);
    when(deleteItem.getAspectName()).thenReturn(DOMAINS_ASPECT_NAME);

    MCPItem keyDeleteItem = mock(MCPItem.class);
    when(keyDeleteItem.getChangeType()).thenReturn(ChangeType.DELETE);
    when(keyDeleteItem.getUrn()).thenReturn(fieldUrn);
    when(keyDeleteItem.getAspectName()).thenReturn(SCHEMA_FIELD_KEY_ASPECT);

    MCPItem upsertItem = mock(MCPItem.class);
    when(upsertItem.getChangeType()).thenReturn(ChangeType.UPSERT);

    step.ingestSideEffectProposals(List.of(deleteItem, keyDeleteItem, upsertItem));

    verify(mockEntityService, times(1))
        .deleteAspect(
            eq(OP_CONTEXT), eq(fieldUrn.toString()), eq(DOMAINS_ASPECT_NAME), any(), eq(false));
    verify(mockEntityService, times(1))
        .deleteAspect(
            eq(OP_CONTEXT), eq(fieldUrn.toString()), eq(SCHEMA_FIELD_KEY_ASPECT), any(), eq(true));
    verify(mockEntityService, times(1))
        .ingestProposal(eq(OP_CONTEXT), any(AspectsBatch.class), eq(true));
  }

  @Test
  public void testIngestSideEffectProposalsNoOpWhenEmpty() {
    GenerateSchemaFieldsFromSchemaMetadataStep step = newStep(false, false, false);
    step.ingestSideEffectProposals(List.of());
    verify(mockEntityService, times(0))
        .ingestProposal(any(), any(AspectsBatch.class), anyBoolean());
    verify(mockEntityService, times(0))
        .ingestAspects(any(), any(), any(Boolean.class), any(Boolean.class));
  }

  private GenerateSchemaFieldsFromSchemaMetadataStep newStep(
      boolean reprocess, boolean domainEnabled, boolean ownershipEnabled) {
    return new GenerateSchemaFieldsFromSchemaMetadataStep(
        OP_CONTEXT,
        mockEntityService,
        mockAspectDao,
        10,
        100,
        1000,
        reprocess,
        domainEnabled,
        ownershipEnabled);
  }

  private static SchemaFieldSideEffect schemaFieldSideEffect(
      boolean domainEnabled, boolean ownershipEnabled) {
    List<AspectPluginConfig.EntityAspectName> supported =
        new ArrayList<>(
            List.of(
                AspectPluginConfig.EntityAspectName.builder()
                    .entityName(DATASET_ENTITY_NAME)
                    .aspectName(SCHEMA_METADATA_ASPECT_NAME)
                    .build(),
                AspectPluginConfig.EntityAspectName.builder()
                    .entityName(DATASET_ENTITY_NAME)
                    .aspectName(com.linkedin.metadata.Constants.STATUS_ASPECT_NAME)
                    .build()));
    if (domainEnabled) {
      supported.add(
          AspectPluginConfig.EntityAspectName.builder()
              .entityName(DATASET_ENTITY_NAME)
              .aspectName(DOMAINS_ASPECT_NAME)
              .build());
    }
    if (ownershipEnabled) {
      supported.add(
          AspectPluginConfig.EntityAspectName.builder()
              .entityName(DATASET_ENTITY_NAME)
              .aspectName(OWNERSHIP_ASPECT_NAME)
              .build());
    }

    EntityChangeEventGeneratorRegistry changeEventRegistry =
        new EntityChangeEventGeneratorRegistry();
    changeEventRegistry.register(
        SCHEMA_METADATA_ASPECT_NAME, new SchemaMetadataChangeEventGenerator());

    return new SchemaFieldSideEffect()
        .setConfig(
            AspectPluginConfig.builder()
                .enabled(true)
                .className(SchemaFieldSideEffect.class.getName())
                .supportedOperations(
                    List.of("CREATE", "CREATE_ENTITY", "UPSERT", "RESTATE", "DELETE"))
                .supportedEntityAspectNames(supported)
                .build())
        .setDomainEnabled(domainEnabled)
        .setOwnershipEnabled(ownershipEnabled)
        .setEntityChangeEventGeneratorRegistry(changeEventRegistry);
  }

  private static SystemAspect mockSchemaMetadataSystemAspect(
      OperationContext opContext, int fieldCount) {
    EntitySpec datasetSpec = opContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME);
    SchemaMetadata schemaMetadata = schemaMetadata(fieldCount);
    SystemAspect systemAspect = mock(SystemAspect.class);
    when(systemAspect.getUrn()).thenReturn(DATASET_URN);
    when(systemAspect.getEntitySpec()).thenReturn(datasetSpec);
    when(systemAspect.getAspectName()).thenReturn(SCHEMA_METADATA_ASPECT_NAME);
    when(systemAspect.getAspectSpec())
        .thenReturn(datasetSpec.getAspectSpec(SCHEMA_METADATA_ASPECT_NAME));
    when(systemAspect.getRecordTemplate()).thenReturn(schemaMetadata);
    when(systemAspect.getAuditStamp()).thenReturn(AUDIT_STAMP);
    when(systemAspect.getSystemMetadata()).thenReturn(new SystemMetadata());
    return systemAspect;
  }

  private static Set<Urn> expectedFieldUrns(int fieldCount) {
    return java.util.stream.IntStream.range(0, fieldCount)
        .mapToObj(
            i ->
                UrnUtils.getUrn(
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),field_"
                        + i
                        + ")"))
        .collect(Collectors.toSet());
  }

  private static SchemaMetadata schemaMetadata(int fieldCount) {
    SchemaFieldArray fields = new SchemaFieldArray();
    for (int i = 0; i < fieldCount; i++) {
      fields.add(
          new SchemaField()
              .setFieldPath("field_" + i)
              .setNativeDataType(i % 2 == 0 ? "string" : "boolean")
              .setType(
                  new SchemaFieldDataType()
                      .setType(
                          i % 2 == 0
                              ? SchemaFieldDataType.Type.create(new StringType())
                              : SchemaFieldDataType.Type.create(new BooleanType()))));
    }
    return new SchemaMetadata()
        .setSchemaName("test")
        .setPlatform(new DataPlatformUrn("hive"))
        .setVersion(0L)
        .setHash("")
        .setPlatformSchema(
            SchemaMetadata.PlatformSchema.create(
                new com.linkedin.schema.OtherSchema().setRawSchema("{}")))
        .setFields(fields);
  }
}
