package com.linkedin.metadata.entity.ebean.batch;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.Status;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.patch.builder.DatasetPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ReadPreference;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class AspectsBatchImplTest {
  private EntityRegistry testRegistry;
  private CachingAspectRetriever mockAspectRetriever;
  private RetrieverContext retrieverContext;

  @BeforeTest
  public void beforeTest() throws EntityRegistryException {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);

    EntityRegistry snapshotEntityRegistry = new SnapshotEntityRegistry();
    EntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            Snapshot.class.getClassLoader().getResourceAsStream("AspectsBatchImplTest.yaml"));
    this.testRegistry =
        new MergedEntityRegistry(snapshotEntityRegistry).apply(configEntityRegistry);
  }

  @BeforeMethod
  public void setup() {
    this.mockAspectRetriever = mock(CachingAspectRetriever.class);
    when(this.mockAspectRetriever.getEntityRegistry()).thenReturn(testRegistry);
    this.retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .cachingAspectRetriever(mockAspectRetriever)
            .graphRetriever(mock(GraphRetriever.class))
            .build();
  }

  @Test
  public void buildValidatesWithPrimaryReadPreference() {
    AtomicReference<OperationFingerprint> seen = new AtomicReference<>();
    AspectPayloadValidator validator =
        new AspectPayloadValidator() {
          @Override
          protected Stream<AspectValidationException> validateProposedAspects(
              @Nonnull OperationFingerprint operationContext,
              @Nonnull Collection<? extends BatchItem> mcpItems,
              @Nonnull com.linkedin.metadata.aspect.RetrieverContext retrieverContext) {
            seen.set(operationContext);
            return Stream.empty();
          }

          @Override
          protected Stream<AspectValidationException> validatePreCommitAspects(
              @Nonnull OperationFingerprint operationContext,
              @Nonnull Collection<ChangeMCP> changeMCPs,
              @Nonnull com.linkedin.metadata.aspect.RetrieverContext retrieverContext) {
            return Stream.empty();
          }

          @Override
          public AspectPluginConfig getConfig() {
            return null;
          }

          @Override
          public AspectPayloadValidator setConfig(@Nonnull AspectPluginConfig config) {
            return this;
          }
        };
    EntityRegistry registry = mock(EntityRegistry.class);
    when(registry.getAllAspectPayloadValidators()).thenReturn(List.of(validator));
    CachingAspectRetriever retriever = mock(CachingAspectRetriever.class);
    when(retriever.getEntityRegistry()).thenReturn(registry);
    RetrieverContext context =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .cachingAspectRetriever(retriever)
            .graphRetriever(mock(GraphRetriever.class))
            .build();
    OperationContext readContext =
        TestOperationContexts.systemContextNoSearchAuthorization()
            .withReadPreference(ReadPreference.READ);

    AspectsBatchImpl.builder().items(List.of()).retrieverContext(context).build(readContext);

    assertNotNull(seen.get());
    assertEquals(
        ((OperationContext) seen.get()).getPrimaryStorageContext().getReadPreference(),
        ReadPreference.PRIMARY);
  }

  @Test
  public void toUpsertBatchItemsChangeItemTest() {
    List<ChangeItemImpl> testItems =
        List.of(
            ChangeItemImpl.builder()
                .urn(
                    UrnUtils.getUrn(
                        "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"))
                .changeType(ChangeType.UPSERT)
                .aspectName(STATUS_ASPECT_NAME)
                .entitySpec(testRegistry.getEntitySpec(DATASET_ENTITY_NAME))
                .aspectSpec(
                    testRegistry
                        .getEntitySpec(DATASET_ENTITY_NAME)
                        .getAspectSpec(STATUS_ASPECT_NAME))
                .auditStamp(AuditStampUtils.createDefaultAuditStamp())
                .recordTemplate(new Status().setRemoved(true))
                .build(mockAspectRetriever),
            ChangeItemImpl.builder()
                .urn(
                    UrnUtils.getUrn(
                        "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"))
                .changeType(ChangeType.UPSERT)
                .aspectName(STATUS_ASPECT_NAME)
                .entitySpec(testRegistry.getEntitySpec(DATASET_ENTITY_NAME))
                .aspectSpec(
                    testRegistry
                        .getEntitySpec(DATASET_ENTITY_NAME)
                        .getAspectSpec(STATUS_ASPECT_NAME))
                .auditStamp(AuditStampUtils.createDefaultAuditStamp())
                .recordTemplate(new Status().setRemoved(false))
                .build(mockAspectRetriever));

    AspectsBatchImpl testBatch =
        AspectsBatchImpl.builder().items(testItems).retrieverContext(retrieverContext).build(null);

    assertEquals(
        testBatch.toUpsertBatchItems(
            OperationFingerprint.EMPTY,
            new HashMap<>(),
            new HashMap<>(),
            (changeMCP, systemAspect) -> systemAspect),
        Pair.of(Map.of(), testItems),
        "Expected noop, pass through with no additional MCPs or changes");
  }

  @Test
  public void toUpsertBatchItemsPatchItemTest() {
    GenericJsonPatch.PatchOp testPatchOp = new GenericJsonPatch.PatchOp();
    testPatchOp.setOp(PatchOperationType.REMOVE.getValue());
    testPatchOp.setPath(
        String.format(
            "/properties/%s", "urn:li:structuredProperty:io.acryl.privacy.retentionTime"));

    List<PatchItemImpl> testItems =
        List.of(
            PatchItemImpl.builder()
                .urn(
                    UrnUtils.getUrn(
                        "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"))
                .entitySpec(testRegistry.getEntitySpec(DATASET_ENTITY_NAME))
                .aspectName(STRUCTURED_PROPERTIES_ASPECT_NAME)
                .aspectSpec(
                    testRegistry
                        .getEntitySpec(DATASET_ENTITY_NAME)
                        .getAspectSpec(STRUCTURED_PROPERTIES_ASPECT_NAME))
                .patch(
                    GenericJsonPatch.builder()
                        .arrayPrimaryKeys(Map.of("properties", List.of("propertyUrn")))
                        .patch(List.of(testPatchOp))
                        .build()
                        .getJsonPatch())
                .auditStamp(AuditStampUtils.createDefaultAuditStamp())
                .build(retrieverContext.getAspectRetriever().getEntityRegistry()),
            PatchItemImpl.builder()
                .urn(
                    UrnUtils.getUrn(
                        "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"))
                .entitySpec(testRegistry.getEntitySpec(DATASET_ENTITY_NAME))
                .aspectName(STRUCTURED_PROPERTIES_ASPECT_NAME)
                .aspectSpec(
                    testRegistry
                        .getEntitySpec(DATASET_ENTITY_NAME)
                        .getAspectSpec(STRUCTURED_PROPERTIES_ASPECT_NAME))
                .patch(
                    GenericJsonPatch.builder()
                        .arrayPrimaryKeys(Map.of("properties", List.of("propertyUrn")))
                        .patch(List.of(testPatchOp))
                        .build()
                        .getJsonPatch())
                .auditStamp(AuditStampUtils.createDefaultAuditStamp())
                .build(retrieverContext.getAspectRetriever().getEntityRegistry()));

    AspectsBatchImpl testBatch =
        AspectsBatchImpl.builder().items(testItems).retrieverContext(retrieverContext).build(null);

    assertEquals(
        testBatch.toUpsertBatchItems(
            OperationFingerprint.EMPTY,
            new HashMap<>(),
            new HashMap<>(),
            (changeMCP, systemAspect) -> systemAspect),
        Pair.of(
            Map.of(),
            List.of(
                ChangeItemImpl.builder()
                    .urn(
                        UrnUtils.getUrn(
                            "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"))
                    .changeType(ChangeType.UPSERT)
                    .aspectName(STRUCTURED_PROPERTIES_ASPECT_NAME)
                    .entitySpec(testRegistry.getEntitySpec(DATASET_ENTITY_NAME))
                    .aspectSpec(
                        testRegistry
                            .getEntitySpec(DATASET_ENTITY_NAME)
                            .getAspectSpec(STRUCTURED_PROPERTIES_ASPECT_NAME))
                    .auditStamp(testItems.get(0).getAuditStamp())
                    .recordTemplate(
                        new StructuredProperties()
                            .setProperties(new StructuredPropertyValueAssignmentArray()))
                    .systemMetadata(testItems.get(0).getSystemMetadata())
                    .build(mockAspectRetriever),
                ChangeItemImpl.builder()
                    .urn(
                        UrnUtils.getUrn(
                            "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"))
                    .changeType(ChangeType.UPSERT)
                    .aspectName(STRUCTURED_PROPERTIES_ASPECT_NAME)
                    .entitySpec(testRegistry.getEntitySpec(DATASET_ENTITY_NAME))
                    .aspectSpec(
                        testRegistry
                            .getEntitySpec(DATASET_ENTITY_NAME)
                            .getAspectSpec(STRUCTURED_PROPERTIES_ASPECT_NAME))
                    .auditStamp(testItems.get(1).getAuditStamp())
                    .recordTemplate(
                        new StructuredProperties()
                            .setProperties(new StructuredPropertyValueAssignmentArray()))
                    .systemMetadata(testItems.get(1).getSystemMetadata())
                    .build(mockAspectRetriever))),
        "Expected patch items converted to upsert change items");
  }

  @Test
  public void toUpsertBatchItemsProposedItemTest() {
    AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();
    List<ProposedItem> testItems =
        List.of(
            ProposedItem.builder()
                .build(
                    new MetadataChangeProposal()
                        .setEntityUrn(
                            UrnUtils.getUrn(
                                "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"))
                        .setAspectName(STATUS_ASPECT_NAME)
                        .setEntityType(DATASET_ENTITY_NAME)
                        .setChangeType(ChangeType.UPSERT)
                        .setAspect(
                            new GenericAspect()
                                .setContentType("application/json")
                                .setValue(
                                    ByteString.copyString(
                                        "{\"foo\":\"bar\",\"removed\":false}",
                                        StandardCharsets.UTF_8))),
                    auditStamp,
                    testRegistry),
            ProposedItem.builder()
                .build(
                    new MetadataChangeProposal()
                        .setEntityUrn(
                            UrnUtils.getUrn(
                                "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"))
                        .setAspectName(STATUS_ASPECT_NAME)
                        .setEntityType(DATASET_ENTITY_NAME)
                        .setChangeType(ChangeType.UPSERT)
                        .setAspect(
                            new GenericAspect()
                                .setContentType("application/json")
                                .setValue(
                                    ByteString.copyString(
                                        "{\"foo\":\"bar\",\"removed\":false}",
                                        StandardCharsets.UTF_8))),
                    auditStamp,
                    testRegistry));

    AspectsBatchImpl testBatch =
        AspectsBatchImpl.builder().items(testItems).retrieverContext(retrieverContext).build(null);

    assertEquals(
        testBatch.toUpsertBatchItems(
            OperationFingerprint.EMPTY,
            new HashMap<>(),
            new HashMap<>(),
            (changeMCP, systemAspect) -> systemAspect),
        Pair.of(
            Map.of(),
            List.of(
                ChangeItemImpl.builder()
                    .urn(
                        UrnUtils.getUrn(
                            "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"))
                    .changeType(ChangeType.UPSERT)
                    .aspectName(STATUS_ASPECT_NAME)
                    .entitySpec(testRegistry.getEntitySpec(DATASET_ENTITY_NAME))
                    .aspectSpec(
                        testRegistry
                            .getEntitySpec(DATASET_ENTITY_NAME)
                            .getAspectSpec(STATUS_ASPECT_NAME))
                    .auditStamp(auditStamp)
                    .systemMetadata(testItems.get(0).getSystemMetadata())
                    .recordTemplate(new Status().setRemoved(false))
                    .build(mockAspectRetriever),
                ChangeItemImpl.builder()
                    .urn(
                        UrnUtils.getUrn(
                            "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"))
                    .changeType(ChangeType.UPSERT)
                    .aspectName(STATUS_ASPECT_NAME)
                    .entitySpec(testRegistry.getEntitySpec(DATASET_ENTITY_NAME))
                    .aspectSpec(
                        testRegistry
                            .getEntitySpec(DATASET_ENTITY_NAME)
                            .getAspectSpec(STATUS_ASPECT_NAME))
                    .auditStamp(auditStamp)
                    .systemMetadata(testItems.get(1).getSystemMetadata())
                    .recordTemplate(new Status().setRemoved(false))
                    .build(mockAspectRetriever))),
        "Mutation to status aspect");
  }

  @Test(expectedExceptions = ValidationException.class)
  public void toUpsertBatchItemsProposedItemInvalidAspectTest() {
    AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();
    ProposedItem.builder()
        .build(
            new MetadataChangeProposal()
                .setEntityUrn(
                    UrnUtils.getUrn(
                        "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"))
                .setAspectName("my-custom-aspect")
                .setEntityType(DATASET_ENTITY_NAME)
                .setChangeType(ChangeType.UPSERT)
                .setAspect(
                    new GenericAspect()
                        .setContentType("application/json")
                        .setValue(
                            ByteString.copyString("{\"foo\":\"bar\"}", StandardCharsets.UTF_8))),
            auditStamp,
            testRegistry);
  }

  @Test
  public void singleInvalidDoesntBreakBatch() {
    MetadataChangeProposal proposal1 =
        new DatasetPropertiesPatchBuilder()
            .urn(new DatasetUrn(new DataPlatformUrn("platform"), "name", FabricType.PROD))
            .setDescription("something")
            .setName("name")
            .addCustomProperty("prop1", "propVal1")
            .addCustomProperty("prop2", "propVal2")
            .build();
    MetadataChangeProposal proposal2 =
        new MetadataChangeProposal()
            .setEntityType(DATASET_ENTITY_NAME)
            .setAspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .setAspect(GenericRecordUtils.serializeAspect(new DatasetProperties()))
            .setChangeType(ChangeType.UPSERT);

    AspectsBatchImpl testBatch =
        AspectsBatchImpl.builder()
            .mcps(
                ImmutableList.of(proposal1, proposal2),
                AuditStampUtils.createDefaultAuditStamp(),
                retrieverContext)
            .retrieverContext(retrieverContext)
            .build(null);

    assertEquals(
        testBatch
            .toUpsertBatchItems(
                OperationFingerprint.EMPTY,
                new HashMap<>(),
                new HashMap<>(),
                (changeMCP, systemAspect) -> systemAspect)
            .getSecond()
            .size(),
        1,
        "Expected 1 valid mcp to be passed through.");
  }

  private static MetadataChangeProposal statusProposal(String name, String attributedActor) {
    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setEntityUrn(new DatasetUrn(new DataPlatformUrn("platform"), name, FabricType.PROD))
            .setEntityType(DATASET_ENTITY_NAME)
            .setAspectName(STATUS_ASPECT_NAME)
            .setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)))
            .setChangeType(ChangeType.UPSERT);
    if (attributedActor != null) {
      AttributedActorHeaders.stamp(mcp, attributedActor);
    }
    return mcp;
  }

  private static AuditStamp systemAuditStamp() {
    return AuditStampUtils.createAuditStamp(SYSTEM_ACTOR);
  }

  @Test
  public void mixedActorsInOneBatch() {
    AspectsBatchImpl batch =
        AspectsBatchImpl.builder()
            .mcps(
                List.of(
                    statusProposal("a", "urn:li:corpuser:alice"),
                    statusProposal("b", "urn:li:corpuser:bob"),
                    statusProposal("c", null)),
                systemAuditStamp(),
                retrieverContext,
                false,
                true)
            .build(null);

    List<BatchItem> items = List.copyOf(batch.getItems());
    assertEquals(items.get(0).getAuditStamp().getActor().toString(), "urn:li:corpuser:alice");
    assertEquals(items.get(0).getAuditStamp().getImpersonator().toString(), SYSTEM_ACTOR);
    assertEquals(items.get(1).getAuditStamp().getActor().toString(), "urn:li:corpuser:bob");
    assertEquals(items.get(1).getAuditStamp().getImpersonator().toString(), SYSTEM_ACTOR);
    assertEquals(items.get(2).getAuditStamp().getActor().toString(), SYSTEM_ACTOR);
  }

  @Test
  public void patchItemCarriesAttributedActor() {
    MetadataChangeProposal patch =
        new DatasetPropertiesPatchBuilder()
            .urn(new DatasetUrn(new DataPlatformUrn("platform"), "name", FabricType.PROD))
            .setDescription("something")
            .build();
    AttributedActorHeaders.stamp(patch, "urn:li:corpuser:alice");

    AspectsBatchImpl batch =
        AspectsBatchImpl.builder()
            .mcps(List.of(patch), systemAuditStamp(), retrieverContext, false, true)
            .build(null);

    BatchItem item = batch.getItems().iterator().next();
    assertEquals(item.getClass(), PatchItemImpl.class);
    assertEquals(item.getAuditStamp().getActor().toString(), "urn:li:corpuser:alice");
  }

  @Test
  public void fourArgOverloadDoesNotTrustHeader() {
    AspectsBatchImpl batch =
        AspectsBatchImpl.builder()
            .mcps(
                List.of(statusProposal("a", "urn:li:corpuser:alice")),
                systemAuditStamp(),
                retrieverContext,
                false)
            .build(null);

    assertEquals(
        batch.getItems().iterator().next().getAuditStamp().getActor().toString(), SYSTEM_ACTOR);
  }

  private static MetadataChangeProposal datasetStatus(String name) {
    return new MetadataChangeProposal()
        .setEntityUrn(new DatasetUrn(new DataPlatformUrn("platform"), name, FabricType.PROD))
        .setEntityType(DATASET_ENTITY_NAME)
        .setAspectName(STATUS_ASPECT_NAME)
        .setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)))
        .setChangeType(ChangeType.UPSERT);
  }

  /** Fails every proposal for a dataset named "rejected*" and filters out "filtered*" ones. */
  private void registerNameBasedValidator() {
    AspectPayloadValidator validator =
        new AspectPayloadValidator() {
          @Override
          protected Stream<AspectValidationException> validateProposedAspects(
              @Nonnull OperationFingerprint operationContext,
              @Nonnull Collection<? extends BatchItem> mcpItems,
              @Nonnull com.linkedin.metadata.aspect.RetrieverContext retrieverContext) {
            return mcpItems.stream()
                .flatMap(
                    item -> {
                      String name = item.getUrn().getEntityKey().get(1);
                      if (name.startsWith("rejected")) {
                        return Stream.of(AspectValidationException.forItem(item, "bad"));
                      }
                      if (name.startsWith("filtered")) {
                        return Stream.of(AspectValidationException.forFilter(item, "skip"));
                      }
                      return Stream.empty();
                    });
          }

          @Override
          protected Stream<AspectValidationException> validatePreCommitAspects(
              @Nonnull OperationFingerprint operationContext,
              @Nonnull Collection<ChangeMCP> changeMCPs,
              @Nonnull com.linkedin.metadata.aspect.RetrieverContext retrieverContext) {
            return Stream.empty();
          }

          @Override
          public AspectPluginConfig getConfig() {
            return AspectPluginConfig.builder()
                .className("nameBasedValidator")
                .enabled(true)
                .supportedOperations(List.of("UPSERT"))
                .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
                .build();
          }

          @Override
          public AspectPayloadValidator setConfig(@Nonnull AspectPluginConfig config) {
            return this;
          }
        };
    EntityRegistry registry = spy(testRegistry);
    doReturn(List.of(validator)).when(registry).getAllAspectPayloadValidators();
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(registry);
  }

  @Test
  public void mcpsRecordingRejectedKeepsProposalsThatCannotBeBuilt() {
    MetadataChangeProposal valid = datasetStatus("valid");
    MetadataChangeProposal malformedUrn =
        datasetStatus("malformed")
            .setEntityUrn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,t,INVALID)"));
    MetadataChangeProposal unknownAspect = datasetStatus("unknown").setAspectName("INVALID");

    AspectsBatchImpl batch =
        AspectsBatchImpl.builder()
            .mcpsRecordingRejected(
                List.of(valid, malformedUrn, unknownAspect),
                AuditStampUtils.createDefaultAuditStamp(),
                retrieverContext,
                false,
                false)
            .buildSkippingRejected(null);

    assertEquals(batch.getItems().size(), 1);
    assertEquals(
        batch.getRejectedProposals().stream().map(Pair::getFirst).toList(),
        List.of(malformedUrn, unknownAspect));
  }

  @Test
  public void buildSkippingRejectedRejectsFatalProposalsAndDropsFilteredOnes() {
    registerNameBasedValidator();
    MetadataChangeProposal kept = datasetStatus("kept");
    MetadataChangeProposal rejected = datasetStatus("rejected");
    MetadataChangeProposal filtered = datasetStatus("filtered");

    AspectsBatchImpl batch =
        AspectsBatchImpl.builder()
            .mcpsRecordingRejected(
                List.of(kept, rejected, filtered),
                AuditStampUtils.createDefaultAuditStamp(),
                retrieverContext,
                false,
                false)
            .buildSkippingRejected(null);

    assertEquals(
        batch.getItems().stream()
            .map(item -> ((MCPItem) item).getMetadataChangeProposal())
            .toList(),
        List.of(kept));
    assertEquals(batch.getRejectedProposals().size(), 1);
    assertSame(batch.getRejectedProposals().get(0).getFirst(), rejected);
    assertTrue(batch.getRejectedProposals().get(0).getSecond() instanceof ValidationException);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void buildStillFailsTheWholeBatch() {
    registerNameBasedValidator();

    AspectsBatchImpl.builder()
        .mcps(
            List.of(datasetStatus("kept"), datasetStatus("rejected")),
            AuditStampUtils.createDefaultAuditStamp(),
            retrieverContext)
        .build(null);
  }

  /** Converts unsupported to status aspect */
  @Getter
  @Setter
  @Accessors(chain = true)
  public static class TestMutator extends MutationHook {
    private AspectPluginConfig config;

    @Override
    protected Stream<MCPItem> proposalMutation(
        @Nonnull OperationFingerprint operationContext,
        @Nonnull Collection<MCPItem> mcpItems,
        @Nonnull com.linkedin.metadata.aspect.RetrieverContext retrieverContext) {
      return mcpItems.stream()
          .peek(
              item ->
                  item.getMetadataChangeProposal()
                      .setAspectName(STATUS_ASPECT_NAME)
                      .setAspect(
                          GenericRecordUtils.serializeAspect(new Status().setRemoved(false))));
    }
  }
}
