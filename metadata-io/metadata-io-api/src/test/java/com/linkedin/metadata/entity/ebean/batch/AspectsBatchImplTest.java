package com.linkedin.metadata.entity.ebean.batch;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

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
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.patch.builder.DatasetPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.entity.SearchRetriever;
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
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.RetrieverContext;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        AspectsBatchImpl.builder().items(testItems).retrieverContext(retrieverContext).build();

    assertEquals(
        testBatch.toUpsertBatchItems(new HashMap<>(), new HashMap<>()),
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
        AspectsBatchImpl.builder().items(testItems).retrieverContext(retrieverContext).build();

    assertEquals(
        testBatch.toUpsertBatchItems(new HashMap<>(), new HashMap<>()),
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
                    .systemMetadata(testItems.get(0).getSystemMetadata().setVersion("1"))
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
                    .systemMetadata(testItems.get(1).getSystemMetadata().setVersion("1"))
                    .build(mockAspectRetriever))),
        "Expected patch items converted to upsert change items");
  }

  @Test
  public void toUpsertBatchItemsProposedItemTest() {
    AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();
    List<ProposedItem> testItems =
        List.of(
            ProposedItem.builder()
                .entitySpec(testRegistry.getEntitySpec(DATASET_ENTITY_NAME))
                .metadataChangeProposal(
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
                                    ByteString.copyString(
                                        "{\"foo\":\"bar\"}", StandardCharsets.UTF_8)))
                        .setSystemMetadata(new SystemMetadata()))
                .auditStamp(auditStamp)
                .build(),
            ProposedItem.builder()
                .entitySpec(testRegistry.getEntitySpec(DATASET_ENTITY_NAME))
                .metadataChangeProposal(
                    new MetadataChangeProposal()
                        .setEntityUrn(
                            UrnUtils.getUrn(
                                "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"))
                        .setAspectName("my-custom-aspect")
                        .setEntityType(DATASET_ENTITY_NAME)
                        .setChangeType(ChangeType.UPSERT)
                        .setAspect(
                            new GenericAspect()
                                .setContentType("application/json")
                                .setValue(
                                    ByteString.copyString(
                                        "{\"foo\":\"bar\"}", StandardCharsets.UTF_8)))
                        .setSystemMetadata(new SystemMetadata()))
                .auditStamp(auditStamp)
                .build());

    AspectsBatchImpl testBatch =
        AspectsBatchImpl.builder().items(testItems).retrieverContext(retrieverContext).build();

    assertEquals(
        testBatch.toUpsertBatchItems(new HashMap<>(), new HashMap<>()),
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
                    .systemMetadata(testItems.get(0).getSystemMetadata().setVersion("1"))
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
                    .systemMetadata(testItems.get(1).getSystemMetadata().setVersion("1"))
                    .recordTemplate(new Status().setRemoved(false))
                    .build(mockAspectRetriever))),
        "Mutation to status aspect");
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
            .build();

    assertEquals(
        testBatch.toUpsertBatchItems(new HashMap<>(), new HashMap<>()).getSecond().size(),
        1,
        "Expected 1 valid mcp to be passed through.");
  }

  /** Converts unsupported to status aspect */
  @Getter
  @Setter
  @Accessors(chain = true)
  public static class TestMutator extends MutationHook {
    private AspectPluginConfig config;

    @Override
    protected Stream<MCPItem> proposalMutation(
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
