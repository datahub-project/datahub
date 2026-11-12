package com.linkedin.metadata.schemafields.sideeffects;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ALIASES_ASPECT;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_KEY_ASPECT;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.hooks.DomainsSyncMutationHook;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.DeleteItemImpl;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGeneratorRegistry;
import com.linkedin.metadata.timeline.eventgenerator.EntityKeyChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.SchemaMetadataChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.StatusChangeEventGenerator;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schemafield.SchemaFieldAliases;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.metadata.context.RetrieverContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SchemaFieldSideEffectTest {
  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();
  private static final List<ChangeType> SUPPORTED_CHANGE_TYPES =
      List.of(
          ChangeType.CREATE,
          ChangeType.CREATE_ENTITY,
          ChangeType.UPSERT,
          ChangeType.DELETE,
          ChangeType.RESTATE);
  private static final Urn TEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)");
  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(SchemaFieldSideEffect.class.getName())
          .enabled(true)
          .supportedOperations(
              SUPPORTED_CHANGE_TYPES.stream()
                  .map(ChangeType::toString)
                  .collect(Collectors.toList()))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName(DATASET_ENTITY_NAME)
                      .aspectName(SCHEMA_METADATA_ASPECT_NAME)
                      .build(),
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName(DATASET_ENTITY_NAME)
                      .aspectName(STATUS_ASPECT_NAME)
                      .build()))
          .build();

  private CachingAspectRetriever mockAspectRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    mockAspectRetriever = mock(CachingAspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_REGISTRY);
    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .cachingAspectRetriever(mockAspectRetriever)
            .graphRetriever(GraphRetriever.EMPTY)
            .build();
  }

  @Test
  public void schemaMetadataToSchemaFieldKeyTest() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    SchemaMetadata schemaMetadata = getTestSchemaMetadata();

    List<MCPItem> testOutput;
    for (ChangeType changeType :
        List.of(ChangeType.CREATE, ChangeType.CREATE_ENTITY, ChangeType.UPSERT)) {
      // Run test
      ChangeItemImpl schemaMetadataChangeItem =
          ChangeItemImpl.builder()
              .urn(TEST_URN)
              .aspectName(SCHEMA_METADATA_ASPECT_NAME)
              .changeType(changeType)
              .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
              .aspectSpec(
                  TEST_REGISTRY
                      .getEntitySpec(DATASET_ENTITY_NAME)
                      .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
              .recordTemplate(schemaMetadata)
              .auditStamp(AuditStampUtils.createDefaultAuditStamp())
              .build(mockAspectRetriever);
      testOutput =
          test.postMCPSideEffect(
                  OperationFingerprint.EMPTY,
                  List.of(
                      MCLItemImpl.builder()
                          .build(
                              schemaMetadataChangeItem,
                              null,
                              null,
                              retrieverContext.getAspectRetriever())),
                  retrieverContext)
              .toList();

      // Verify test
      switch (changeType) {
        default -> {
          assertEquals(
              testOutput.size(), 2, "Unexpected output items for changeType:" + changeType);

          assertEquals(
              testOutput,
              List.of(
                  ChangeItemImpl.builder()
                      .urn(
                          UrnUtils.getUrn(
                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)"))
                      .aspectName(SCHEMA_FIELD_ALIASES_ASPECT)
                      .changeType(ChangeType.UPSERT)
                      .entitySpec(TEST_REGISTRY.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME))
                      .aspectSpec(
                          TEST_REGISTRY
                              .getEntitySpec(SCHEMA_FIELD_ENTITY_NAME)
                              .getAspectSpec(SCHEMA_FIELD_ALIASES_ASPECT))
                      .recordTemplate(
                          new SchemaFieldAliases()
                              .setAliases(
                                  new UrnArray(
                                      List.of(
                                          UrnUtils.getUrn(
                                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)")))))
                      .auditStamp(schemaMetadataChangeItem.getAuditStamp())
                      .systemMetadata(schemaMetadataChangeItem.getSystemMetadata())
                      .build(mockAspectRetriever),
                  ChangeItemImpl.builder()
                      .urn(
                          UrnUtils.getUrn(
                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_name)"))
                      .aspectName(SCHEMA_FIELD_ALIASES_ASPECT)
                      .changeType(ChangeType.UPSERT)
                      .entitySpec(TEST_REGISTRY.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME))
                      .aspectSpec(
                          TEST_REGISTRY
                              .getEntitySpec(SCHEMA_FIELD_ENTITY_NAME)
                              .getAspectSpec(SCHEMA_FIELD_ALIASES_ASPECT))
                      .recordTemplate(
                          new SchemaFieldAliases()
                              .setAliases(
                                  new UrnArray(
                                      List.of(
                                          UrnUtils.getUrn(
                                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_name)")))))
                      .auditStamp(schemaMetadataChangeItem.getAuditStamp())
                      .systemMetadata(schemaMetadataChangeItem.getSystemMetadata())
                      .build(mockAspectRetriever)));
        }
      }
    }
  }

  @Test
  public void statusToSchemaFieldStatusTest() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    SchemaMetadata schemaMetadata = getTestSchemaMetadata();
    Status status = new Status().setRemoved(true);

    // Case 1. schemaMetadata (exists), then status updated
    reset(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_REGISTRY);
    when(mockAspectRetriever.getLatestAspectObjects(
            any(OperationFingerprint.class),
            eq(Set.of(TEST_URN)),
            eq(Set.of(SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(
            Map.of(
                TEST_URN, Map.of(SCHEMA_METADATA_ASPECT_NAME, new Aspect(schemaMetadata.data()))));

    List<MCPItem> testOutput;
    for (ChangeType changeType : List.of(ChangeType.CREATE, ChangeType.UPSERT)) {
      // Run Status test
      ChangeItemImpl statusChangeItem =
          ChangeItemImpl.builder()
              .urn(TEST_URN)
              .aspectName(STATUS_ASPECT_NAME)
              .changeType(changeType)
              .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
              .aspectSpec(
                  TEST_REGISTRY
                      .getEntitySpec(DATASET_ENTITY_NAME)
                      .getAspectSpec(STATUS_ASPECT_NAME))
              .recordTemplate(status)
              .auditStamp(AuditStampUtils.createDefaultAuditStamp())
              .build(mockAspectRetriever);
      testOutput =
          test.postMCPSideEffect(
                  OperationFingerprint.EMPTY,
                  List.of(
                      MCLItemImpl.builder()
                          .build(
                              statusChangeItem, null, null, retrieverContext.getAspectRetriever())),
                  retrieverContext)
              .collect(Collectors.toList());

      // Verify test
      switch (changeType) {
        default -> {
          assertEquals(
              testOutput.size(), 2, "Unexpected output items for changeType:" + changeType);

          assertEquals(
              testOutput,
              List.of(
                  ChangeItemImpl.builder()
                      .urn(
                          UrnUtils.getUrn(
                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)"))
                      .aspectName(STATUS_ASPECT_NAME)
                      .changeType(ChangeType.UPSERT)
                      .entitySpec(TEST_REGISTRY.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME))
                      .aspectSpec(
                          TEST_REGISTRY
                              .getEntitySpec(SCHEMA_FIELD_ENTITY_NAME)
                              .getAspectSpec(STATUS_ASPECT_NAME))
                      .recordTemplate(status)
                      .auditStamp(statusChangeItem.getAuditStamp())
                      .systemMetadata(statusChangeItem.getSystemMetadata())
                      .build(mockAspectRetriever),
                  ChangeItemImpl.builder()
                      .urn(
                          UrnUtils.getUrn(
                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_name)"))
                      .aspectName(STATUS_ASPECT_NAME)
                      .changeType(ChangeType.UPSERT)
                      .entitySpec(TEST_REGISTRY.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME))
                      .aspectSpec(
                          TEST_REGISTRY
                              .getEntitySpec(SCHEMA_FIELD_ENTITY_NAME)
                              .getAspectSpec(STATUS_ASPECT_NAME))
                      .recordTemplate(status)
                      .auditStamp(statusChangeItem.getAuditStamp())
                      .systemMetadata(statusChangeItem.getSystemMetadata())
                      .build(mockAspectRetriever)));
        }
      }
    }

    // Case 2. status (exists), then schemaMetadata
    reset(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_REGISTRY);
    when(mockAspectRetriever.getLatestAspectObjects(
            any(OperationFingerprint.class), eq(Set.of(TEST_URN)), eq(Set.of(STATUS_ASPECT_NAME))))
        .thenReturn(Map.of(TEST_URN, Map.of(STATUS_ASPECT_NAME, new Aspect(status.data()))));

    for (ChangeType changeType : List.of(ChangeType.CREATE, ChangeType.UPSERT)) {
      // Run test
      ChangeItemImpl schemaMetadataChangeItem =
          ChangeItemImpl.builder()
              .urn(TEST_URN)
              .aspectName(SCHEMA_METADATA_ASPECT_NAME)
              .changeType(changeType)
              .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
              .aspectSpec(
                  TEST_REGISTRY
                      .getEntitySpec(DATASET_ENTITY_NAME)
                      .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
              .recordTemplate(schemaMetadata)
              .auditStamp(AuditStampUtils.createDefaultAuditStamp())
              .build(mockAspectRetriever);
      testOutput =
          test.postMCPSideEffect(
                  OperationFingerprint.EMPTY,
                  List.of(
                      MCLItemImpl.builder()
                          .build(
                              schemaMetadataChangeItem,
                              null,
                              null,
                              retrieverContext.getAspectRetriever())),
                  retrieverContext)
              .collect(Collectors.toList());

      // Verify test
      switch (changeType) {
        default -> {
          assertEquals(
              testOutput.size(), 4, "Unexpected output items for changeType:" + changeType);

          assertEquals(
              testOutput,
              List.of(
                  ChangeItemImpl.builder()
                      .urn(
                          UrnUtils.getUrn(
                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)"))
                      .aspectName(STATUS_ASPECT_NAME)
                      .changeType(ChangeType.UPSERT)
                      .entitySpec(TEST_REGISTRY.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME))
                      .aspectSpec(
                          TEST_REGISTRY
                              .getEntitySpec(SCHEMA_FIELD_ENTITY_NAME)
                              .getAspectSpec(STATUS_ASPECT_NAME))
                      .recordTemplate(status)
                      .auditStamp(schemaMetadataChangeItem.getAuditStamp())
                      .systemMetadata(schemaMetadataChangeItem.getSystemMetadata())
                      .build(mockAspectRetriever),
                  ChangeItemImpl.builder()
                      .urn(
                          UrnUtils.getUrn(
                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_name)"))
                      .aspectName(STATUS_ASPECT_NAME)
                      .changeType(ChangeType.UPSERT)
                      .entitySpec(TEST_REGISTRY.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME))
                      .aspectSpec(
                          TEST_REGISTRY
                              .getEntitySpec(SCHEMA_FIELD_ENTITY_NAME)
                              .getAspectSpec(STATUS_ASPECT_NAME))
                      .recordTemplate(status)
                      .auditStamp(schemaMetadataChangeItem.getAuditStamp())
                      .systemMetadata(schemaMetadataChangeItem.getSystemMetadata())
                      .build(mockAspectRetriever),
                  ChangeItemImpl.builder()
                      .urn(
                          UrnUtils.getUrn(
                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)"))
                      .aspectName(SCHEMA_FIELD_ALIASES_ASPECT)
                      .changeType(ChangeType.UPSERT)
                      .entitySpec(TEST_REGISTRY.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME))
                      .aspectSpec(
                          TEST_REGISTRY
                              .getEntitySpec(SCHEMA_FIELD_ENTITY_NAME)
                              .getAspectSpec(SCHEMA_FIELD_ALIASES_ASPECT))
                      .recordTemplate(
                          new SchemaFieldAliases()
                              .setAliases(
                                  new UrnArray(
                                      List.of(
                                          UrnUtils.getUrn(
                                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)")))))
                      .auditStamp(schemaMetadataChangeItem.getAuditStamp())
                      .systemMetadata(schemaMetadataChangeItem.getSystemMetadata())
                      .build(mockAspectRetriever),
                  ChangeItemImpl.builder()
                      .urn(
                          UrnUtils.getUrn(
                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_name)"))
                      .aspectName(SCHEMA_FIELD_ALIASES_ASPECT)
                      .changeType(ChangeType.UPSERT)
                      .entitySpec(TEST_REGISTRY.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME))
                      .aspectSpec(
                          TEST_REGISTRY
                              .getEntitySpec(SCHEMA_FIELD_ENTITY_NAME)
                              .getAspectSpec(SCHEMA_FIELD_ALIASES_ASPECT))
                      .recordTemplate(
                          new SchemaFieldAliases()
                              .setAliases(
                                  new UrnArray(
                                      List.of(
                                          UrnUtils.getUrn(
                                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_name)")))))
                      .auditStamp(schemaMetadataChangeItem.getAuditStamp())
                      .systemMetadata(schemaMetadataChangeItem.getSystemMetadata())
                      .build(mockAspectRetriever)));
        }
      }
    }
  }

  @Test
  public void schemaMetadataDeleteTest() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    SchemaMetadata schemaMetadata = getTestSchemaMetadata();

    // Run test
    MCLItem schemaMetadataChangeItem =
        MCLItemImpl.builder()
            .metadataChangeLog(
                new MetadataChangeLog()
                    .setChangeType(ChangeType.DELETE)
                    .setEntityUrn(TEST_URN)
                    .setEntityType(DATASET_ENTITY_NAME)
                    .setAspectName(SCHEMA_METADATA_ASPECT_NAME)
                    .setPreviousAspectValue(GenericRecordUtils.serializeAspect(schemaMetadata))
                    .setCreated(AuditStampUtils.createDefaultAuditStamp()))
            .build(retrieverContext.getAspectRetriever());

    List<MCPItem> testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY, List.of(schemaMetadataChangeItem), retrieverContext)
            .toList();

    List<MCPItem> expectedEveryAspectPerField = new ArrayList<>();
    for (String schemaField :
        List.of(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)",
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_name)")) {
      for (AspectSpec aspectSpec :
          TEST_REGISTRY.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME).getAspectSpecs()) {
        expectedEveryAspectPerField.add(
            DeleteItemImpl.builder()
                .urn(UrnUtils.getUrn(schemaField))
                .aspectName(aspectSpec.getName())
                .auditStamp(schemaMetadataChangeItem.getAuditStamp())
                .entitySpec(TEST_REGISTRY.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME))
                .aspectSpec(aspectSpec)
                .build(retrieverContext.getAspectRetriever()));
      }
    }

    assertEquals(
        testOutput.size(),
        expectedEveryAspectPerField.size(),
        "Unexpected output items for changeType:" + ChangeType.DELETE);
    assertEquals(
        testOutput.stream()
            .filter(item -> item.getAspectName().equals(SCHEMA_FIELD_KEY_ASPECT))
            .count(),
        2,
        "Expected both key aspects");

    assertEquals(testOutput, expectedEveryAspectPerField);
  }

  @Test
  public void statusDeleteTest() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    Status status = new Status().setRemoved(false);

    // mock response
    reset(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_REGISTRY);
    when(mockAspectRetriever.getLatestAspectObjects(
            any(OperationFingerprint.class), eq(Set.of(TEST_URN)), anySet()))
        .thenReturn(
            Map.of(
                TEST_URN,
                Map.of(SCHEMA_METADATA_ASPECT_NAME, new Aspect(getTestSchemaMetadata().data()))));

    // Run test
    MCLItem statusChangeItem =
        MCLItemImpl.builder()
            .metadataChangeLog(
                new MetadataChangeLog()
                    .setChangeType(ChangeType.DELETE)
                    .setEntityUrn(TEST_URN)
                    .setEntityType(DATASET_ENTITY_NAME)
                    .setAspectName(STATUS_ASPECT_NAME)
                    .setPreviousAspectValue(GenericRecordUtils.serializeAspect(status))
                    .setCreated(AuditStampUtils.createDefaultAuditStamp()))
            .build(retrieverContext.getAspectRetriever());

    List<MCPItem> testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY, List.of(statusChangeItem), retrieverContext)
            .toList();

    List<MCPItem> expectedStatusDeletePerField = new ArrayList<>();
    for (String schemaField :
        List.of(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)",
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_name)")) {
      for (AspectSpec aspectSpec :
          List.of(
              TEST_REGISTRY
                  .getEntitySpec(SCHEMA_FIELD_ENTITY_NAME)
                  .getAspectSpec(STATUS_ASPECT_NAME))) {
        expectedStatusDeletePerField.add(
            DeleteItemImpl.builder()
                .urn(UrnUtils.getUrn(schemaField))
                .aspectName(aspectSpec.getName())
                .auditStamp(statusChangeItem.getAuditStamp())
                .entitySpec(TEST_REGISTRY.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME))
                .aspectSpec(aspectSpec)
                .build(retrieverContext.getAspectRetriever()));
      }
    }

    assertEquals(
        testOutput.size(),
        expectedStatusDeletePerField.size(),
        "Unexpected output items for changeType:" + ChangeType.DELETE);
    assertEquals(
        testOutput.stream().filter(item -> item.getAspectName().equals(STATUS_ASPECT_NAME)).count(),
        2,
        "Expected both status aspects");
    assertEquals(testOutput, expectedStatusDeletePerField);
  }

  @Test
  public void schemaMetadataRemovedFieldTest() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setEntityChangeEventGeneratorRegistry(buildEntityChangeEventGeneratorRegistry());

    SchemaMetadata previousSchemaMetadata = getTestSchemaMetadata();
    SchemaMetadata currentSchemaMetadata = getTestSchemaMetadataWithRemovedField();

    List<MCPItem> testOutput;
    for (ChangeType changeType : List.of(ChangeType.UPSERT)) {
      // Run test
      ChangeItemImpl schemaMetadataChangeItem =
          ChangeItemImpl.builder()
              .urn(TEST_URN)
              .aspectName(SCHEMA_METADATA_ASPECT_NAME)
              .changeType(changeType)
              .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
              .aspectSpec(
                  TEST_REGISTRY
                      .getEntitySpec(DATASET_ENTITY_NAME)
                      .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
              .recordTemplate(currentSchemaMetadata)
              .auditStamp(AuditStampUtils.createDefaultAuditStamp())
              .build(mockAspectRetriever);
      testOutput =
          test.postMCPSideEffect(
                  OperationFingerprint.EMPTY,
                  List.of(
                      MCLItemImpl.builder()
                          .build(
                              schemaMetadataChangeItem,
                              // populate previous item with the now removed field
                              previousSchemaMetadata,
                              null,
                              retrieverContext.getAspectRetriever())),
                  retrieverContext)
              .toList();

      // Verify test
      switch (changeType) {
        default -> {
          assertEquals(
              testOutput.size(), 1, "Unexpected output items for changeType:" + changeType);

          assertEquals(
              testOutput,
              List.of(
                  ChangeItemImpl.builder()
                      .urn(
                          UrnUtils.getUrn(
                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_name)"))
                      .aspectName(STATUS_ASPECT_NAME)
                      .changeType(changeType)
                      .entitySpec(TEST_REGISTRY.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME))
                      .aspectSpec(
                          TEST_REGISTRY
                              .getEntitySpec(SCHEMA_FIELD_ENTITY_NAME)
                              .getAspectSpec(STATUS_ASPECT_NAME))
                      .recordTemplate(new Status().setRemoved(true))
                      .auditStamp(schemaMetadataChangeItem.getAuditStamp())
                      .systemMetadata(schemaMetadataChangeItem.getSystemMetadata())
                      .build(mockAspectRetriever)));
        }
      }
    }
  }

  private static SchemaMetadata getTestSchemaMetadata() {
    String rawSchemaMetadataString =
        "{\"foreignKeys\":[{\"name\":\"user id\",\"sourceFields\":[\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)\"],\"foreignFields\":[\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD),user_id)\"],\"foreignDataset\":\"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)\"}],\"platformSchema\":{\"com.linkedin.schema.KafkaSchema\":{\"documentSchemaType\":\"AVRO\",\"documentSchema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"SampleHiveSchema\\\",\\\"namespace\\\":\\\"com.linkedin.dataset\\\",\\\"doc\\\":\\\"Sample Hive dataset\\\",\\\"fields\\\":[{\\\"name\\\":\\\"field_foo\\\",\\\"type\\\":[\\\"string\\\"]},{\\\"name\\\":\\\"field_bar\\\",\\\"type\\\":[\\\"boolean\\\"]}]}\"}},\"created\":{\"actor\":\"urn:li:corpuser:jdoe\",\"time\":1674291843000},\"lastModified\":{\"actor\":\"urn:li:corpuser:jdoe\",\"time\":1674291843000},\"fields\":[{\"nullable\":false,\"fieldPath\":\"user_id\",\"description\":\"Id of the user created\",\"isPartOfKey\":false,\"type\":{\"type\":{\"com.linkedin.schema.BooleanType\":{}}},\"recursive\":false,\"nativeDataType\":\"varchar(100)\"},{\"nullable\":false,\"fieldPath\":\"user_name\",\"description\":\"Name of the user who signed up\",\"isPartOfKey\":false,\"type\":{\"type\":{\"com.linkedin.schema.BooleanType\":{}}},\"recursive\":false,\"nativeDataType\":\"boolean\"}],\"schemaName\":\"SampleHiveSchema\",\"version\":0,\"hash\":\"\",\"platform\":\"urn:li:dataPlatform:hive\"}";
    ByteString rawSchemaMetadataBytes =
        ByteString.copyString(rawSchemaMetadataString, StandardCharsets.UTF_8);
    return GenericRecordUtils.deserializeAspect(
        rawSchemaMetadataBytes, "application/json", SchemaMetadata.class);
  }

  private static SchemaMetadata getTestSchemaMetadataWithRemovedField() {
    String rawSchemaMetadataString =
        "{\"foreignKeys\":[{\"name\":\"user id\",\"sourceFields\":[\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)\"],\"foreignFields\":[\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD),user_id)\"],\"foreignDataset\":\"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)\"}],\"platformSchema\":{\"com.linkedin.schema.KafkaSchema\":{\"documentSchemaType\":\"AVRO\",\"documentSchema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"SampleHiveSchema\\\",\\\"namespace\\\":\\\"com.linkedin.dataset\\\",\\\"doc\\\":\\\"Sample Hive dataset\\\",\\\"fields\\\":[{\\\"name\\\":\\\"field_foo\\\",\\\"type\\\":[\\\"string\\\"]},{\\\"name\\\":\\\"field_bar\\\",\\\"type\\\":[\\\"boolean\\\"]}]}\"}},\"created\":{\"actor\":\"urn:li:corpuser:jdoe\",\"time\":1674291843000},\"lastModified\":{\"actor\":\"urn:li:corpuser:jdoe\",\"time\":1674291843000},\"fields\":[{\"nullable\":false,\"fieldPath\":\"user_id\",\"description\":\"Id of the user created\",\"isPartOfKey\":false,\"type\":{\"type\":{\"com.linkedin.schema.BooleanType\":{}}},\"recursive\":false,\"nativeDataType\":\"varchar(100)\"}],\"schemaName\":\"SampleHiveSchema\",\"version\":0,\"hash\":\"\",\"platform\":\"urn:li:dataPlatform:hive\"}";
    ByteString rawSchemaMetadataBytes =
        ByteString.copyString(rawSchemaMetadataString, StandardCharsets.UTF_8);
    return GenericRecordUtils.deserializeAspect(
        rawSchemaMetadataBytes, "application/json", SchemaMetadata.class);
  }

  private static EntityChangeEventGeneratorRegistry buildEntityChangeEventGeneratorRegistry() {
    final EntityChangeEventGeneratorRegistry registry = new EntityChangeEventGeneratorRegistry();
    registry.register(SCHEMA_METADATA_ASPECT_NAME, new SchemaMetadataChangeEventGenerator());

    // Entity Lifecycle change event generators
    registry.register(DATASET_KEY_ASPECT_NAME, new EntityKeyChangeEventGenerator<>());
    registry.register(STATUS_ASPECT_NAME, new StatusChangeEventGenerator());

    return registry;
  }

  @Test
  public void schemaMetadataRestateAliasesTest() throws CloneNotSupportedException {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setEntityChangeEventGeneratorRegistry(mock(EntityChangeEventGeneratorRegistry.class));
    test.setConfig(TEST_PLUGIN_CONFIG);
    SchemaMetadata schemaMetadata = getTestSchemaMetadata();

    List<MCPItem> testOutput;
    for (ChangeType changeType : List.of(ChangeType.RESTATE)) {
      // Run test with RESTATE MCP
      SystemMetadata systemMetadata = SystemMetadataUtils.createDefaultSystemMetadata();
      MetadataChangeProposal restateMCP =
          new MetadataChangeProposal()
              .setEntityUrn(TEST_URN)
              .setChangeType(changeType)
              .setAspectName(SCHEMA_METADATA_ASPECT_NAME)
              .setEntityType(TEST_URN.getEntityType())
              .setSystemMetadata(systemMetadata)
              .setAspect(GenericRecordUtils.serializeAspect(schemaMetadata));

      MCPItem schemaMetadataChangeItem =
          TestMCP.builder()
              .urn(TEST_URN)
              .metadataChangeProposal(restateMCP)
              .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
              .changeType(changeType)
              .aspectSpec(
                  TEST_REGISTRY
                      .getEntitySpec(DATASET_ENTITY_NAME)
                      .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
              .auditStamp(AuditStampUtils.createDefaultAuditStamp())
              .systemMetadata(systemMetadata)
              .build();

      testOutput =
          test.postMCPSideEffect(
                  OperationFingerprint.EMPTY,
                  List.of(
                      MCLItemImpl.builder()
                          .build(
                              schemaMetadataChangeItem,
                              schemaMetadata,
                              schemaMetadataChangeItem.getSystemMetadata(),
                              retrieverContext.getAspectRetriever())),
                  retrieverContext)
              .toList();

      // Verify test
      switch (changeType) {
        default -> {
          assertEquals(
              testOutput.size(), 2, "Unexpected output items for changeType:" + changeType);

          assertEquals(
              testOutput,
              List.of(
                  ChangeItemImpl.builder()
                      .urn(
                          UrnUtils.getUrn(
                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)"))
                      .aspectName(SCHEMA_FIELD_ALIASES_ASPECT)
                      .changeType(ChangeType.UPSERT)
                      .entitySpec(TEST_REGISTRY.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME))
                      .aspectSpec(
                          TEST_REGISTRY
                              .getEntitySpec(SCHEMA_FIELD_ENTITY_NAME)
                              .getAspectSpec(SCHEMA_FIELD_ALIASES_ASPECT))
                      .recordTemplate(
                          new SchemaFieldAliases()
                              .setAliases(
                                  new UrnArray(
                                      List.of(
                                          UrnUtils.getUrn(
                                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)")))))
                      .auditStamp(schemaMetadataChangeItem.getAuditStamp())
                      .systemMetadata(systemMetadata)
                      .build(mockAspectRetriever),
                  ChangeItemImpl.builder()
                      .urn(
                          UrnUtils.getUrn(
                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_name)"))
                      .aspectName(SCHEMA_FIELD_ALIASES_ASPECT)
                      .changeType(ChangeType.UPSERT)
                      .entitySpec(TEST_REGISTRY.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME))
                      .aspectSpec(
                          TEST_REGISTRY
                              .getEntitySpec(SCHEMA_FIELD_ENTITY_NAME)
                              .getAspectSpec(SCHEMA_FIELD_ALIASES_ASPECT))
                      .recordTemplate(
                          new SchemaFieldAliases()
                              .setAliases(
                                  new UrnArray(
                                      List.of(
                                          UrnUtils.getUrn(
                                              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_name)")))))
                      .auditStamp(schemaMetadataChangeItem.getAuditStamp())
                      .systemMetadata(systemMetadata)
                      .build(mockAspectRetriever)));
        }
      }
    }
  }

  @Test
  public void domainsToSchemaFieldDomainsTest() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setDomainEnabled(true);
    SchemaMetadata schemaMetadata = getTestSchemaMetadata();
    Domains domains = testDomains();

    stubSchemaMetadataStored(schemaMetadata);

    ChangeItemImpl domainsChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_URN)
            .aspectName(DOMAINS_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME).getAspectSpec(DOMAINS_ASPECT_NAME))
            .recordTemplate(domains)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    List<MCPItem> testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            domainsChangeItem, null, null, retrieverContext.getAspectRetriever())),
                retrieverContext)
            .toList();

    assertEquals(testOutput.size(), 2);
    assertEquals(
        testOutput.stream().map(MCPItem::getAspectName).collect(Collectors.toSet()),
        Set.of(DOMAINS_ASPECT_NAME));
    assertEquals(
        testOutput.stream()
            .map(item -> item.getAspect(Domains.class).getDomains())
            .collect(Collectors.toSet()),
        Set.of(domains.getDomains()));
  }

  @Test
  public void domainsFlagOffEmitsNoDomainMcps() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setDomainEnabled(false);
    SchemaMetadata schemaMetadata = getTestSchemaMetadata();
    Domains domains = testDomains();

    stubSchemaMetadataStored(schemaMetadata);

    ChangeItemImpl domainsChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_URN)
            .aspectName(DOMAINS_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME).getAspectSpec(DOMAINS_ASPECT_NAME))
            .recordTemplate(domains)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    List<MCPItem> testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            domainsChangeItem, null, null, retrieverContext.getAspectRetriever())),
                retrieverContext)
            .toList();

    assertEquals(testOutput.size(), 0);
  }

  @Test
  public void mirroredDomainMcpsHaveIndependentRecordsAfterDomainsSync() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setDomainEnabled(true);
    SchemaMetadata schemaMetadata = getTestSchemaMetadata();
    Domains domains = testDomains();

    stubSchemaMetadataStored(schemaMetadata);

    ChangeItemImpl domainsChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_URN)
            .aspectName(DOMAINS_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME).getAspectSpec(DOMAINS_ASPECT_NAME))
            .recordTemplate(domains)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    List<ChangeMCP> fieldDomainMcps =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            domainsChangeItem, null, null, retrieverContext.getAspectRetriever())),
                retrieverContext)
            .map(item -> (ChangeMCP) item)
            .toList();

    assertEquals(fieldDomainMcps.size(), 2);
    assertNotSame(
        fieldDomainMcps.get(0).getRecordTemplate().data(),
        fieldDomainMcps.get(1).getRecordTemplate().data());

    DomainsSyncMutationHook domainsSync = new DomainsSyncMutationHook();
    domainsSync.setConfig(
        AspectPluginConfig.builder()
            .className(DomainsSyncMutationHook.class.getName())
            .enabled(true)
            .supportedOperations(List.of("UPSERT"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("*")
                        .aspectName(DOMAINS_ASPECT_NAME)
                        .build()))
            .build());
    domainsSync
        .applyWriteMutation(OperationFingerprint.EMPTY, fieldDomainMcps, retrieverContext)
        .toList();

    Domains domains0 = fieldDomainMcps.get(0).getAspect(Domains.class);
    Domains domains1 = fieldDomainMcps.get(1).getAspect(Domains.class);
    assertNotSame(domains0.data(), domains1.data());
    assertTrue(domains0.hasDomainAssociations());
    assertTrue(domains1.hasDomainAssociations());
    assertEquals(domains0.getDomainAssociations(), domains1.getDomainAssociations());

    // Mutating one field's Domains must not leak into the other.
    domains0.setDomains(new UrnArray(List.of(UrnUtils.getUrn("urn:li:domain:other"))));
    assertNotEquals(domains0.getDomains(), domains1.getDomains());
  }

  @Test
  public void ownershipToSchemaFieldOwnershipTest() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setOwnershipEnabled(true);
    SchemaMetadata schemaMetadata = getTestSchemaMetadata();
    Ownership ownership = testOwnership();

    stubSchemaMetadataStored(schemaMetadata);

    ChangeItemImpl ownershipChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_URN)
            .aspectName(OWNERSHIP_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(OWNERSHIP_ASPECT_NAME))
            .recordTemplate(ownership)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    List<MCPItem> testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            ownershipChangeItem,
                            null,
                            null,
                            retrieverContext.getAspectRetriever())),
                retrieverContext)
            .toList();

    assertEquals(testOutput.size(), 2);
    assertEquals(
        testOutput.stream().map(MCPItem::getAspectName).collect(Collectors.toSet()),
        Set.of(OWNERSHIP_ASPECT_NAME));
    assertEquals(
        testOutput.stream()
            .map(item -> item.getAspect(Ownership.class).getOwners())
            .collect(Collectors.toSet()),
        Set.of(ownership.getOwners()));
  }

  @Test
  public void ownershipFlagOffEmitsNoOwnershipMcps() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setOwnershipEnabled(false);
    SchemaMetadata schemaMetadata = getTestSchemaMetadata();
    Ownership ownership = testOwnership();

    stubSchemaMetadataStored(schemaMetadata);

    ChangeItemImpl ownershipChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_URN)
            .aspectName(OWNERSHIP_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(OWNERSHIP_ASPECT_NAME))
            .recordTemplate(ownership)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    List<MCPItem> testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            ownershipChangeItem,
                            null,
                            null,
                            retrieverContext.getAspectRetriever())),
                retrieverContext)
            .toList();

    assertEquals(testOutput.size(), 0);
  }

  @Test
  public void schemaMetadataAfterDomainsMirrorsStoredDomains() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setDomainEnabled(true);
    SchemaMetadata schemaMetadata = getTestSchemaMetadata();
    Domains domains = testDomains();

    stubLatestAspects(Map.of(DOMAINS_ASPECT_NAME, new Aspect(domains.data())));

    ChangeItemImpl schemaMetadataChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_URN)
            .aspectName(SCHEMA_METADATA_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
            .recordTemplate(schemaMetadata)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    List<MCPItem> testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            schemaMetadataChangeItem,
                            null,
                            null,
                            retrieverContext.getAspectRetriever())),
                retrieverContext)
            .toList();

    assertEquals(
        testOutput.stream()
            .filter(item -> DOMAINS_ASPECT_NAME.equals(item.getAspectName()))
            .count(),
        2);
    assertEquals(
        testOutput.stream()
            .filter(item -> DOMAINS_ASPECT_NAME.equals(item.getAspectName()))
            .map(item -> item.getAspect(Domains.class).getDomains())
            .collect(Collectors.toSet()),
        Set.of(domains.getDomains()));
    assertTrue(
        testOutput.stream()
            .anyMatch(item -> SCHEMA_FIELD_ALIASES_ASPECT.equals(item.getAspectName())));
  }

  @Test
  public void schemaMetadataAfterOwnershipMirrorsStoredOwnership() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setOwnershipEnabled(true);
    SchemaMetadata schemaMetadata = getTestSchemaMetadata();
    Ownership ownership = testOwnership();

    stubLatestAspects(Map.of(OWNERSHIP_ASPECT_NAME, new Aspect(ownership.data())));

    ChangeItemImpl schemaMetadataChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_URN)
            .aspectName(SCHEMA_METADATA_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
            .recordTemplate(schemaMetadata)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    List<MCPItem> testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            schemaMetadataChangeItem,
                            null,
                            null,
                            retrieverContext.getAspectRetriever())),
                retrieverContext)
            .toList();

    assertEquals(
        testOutput.stream()
            .filter(item -> OWNERSHIP_ASPECT_NAME.equals(item.getAspectName()))
            .count(),
        2);
    assertEquals(
        testOutput.stream()
            .filter(item -> OWNERSHIP_ASPECT_NAME.equals(item.getAspectName()))
            .map(item -> item.getAspect(Ownership.class).getOwners())
            .collect(Collectors.toSet()),
        Set.of(ownership.getOwners()));
  }

  @Test
  public void domainsDeleteMirrorsToSchemaFields() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setDomainEnabled(true);
    Domains domains = testDomains();

    stubSchemaMetadataStored(getTestSchemaMetadata());

    MCLItem domainsDeleteItem =
        MCLItemImpl.builder()
            .metadataChangeLog(
                new MetadataChangeLog()
                    .setChangeType(ChangeType.DELETE)
                    .setEntityUrn(TEST_URN)
                    .setEntityType(DATASET_ENTITY_NAME)
                    .setAspectName(DOMAINS_ASPECT_NAME)
                    .setPreviousAspectValue(GenericRecordUtils.serializeAspect(domains))
                    .setCreated(AuditStampUtils.createDefaultAuditStamp()))
            .build(retrieverContext.getAspectRetriever());

    List<MCPItem> testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY, List.of(domainsDeleteItem), retrieverContext)
            .toList();

    assertEquals(testOutput.size(), 2);
    assertEquals(
        testOutput.stream().map(MCPItem::getAspectName).collect(Collectors.toSet()),
        Set.of(DOMAINS_ASPECT_NAME));
    assertTrue(testOutput.stream().allMatch(item -> item instanceof DeleteItemImpl));
    assertEquals(
        testOutput.stream().map(item -> item.getUrn().toString()).collect(Collectors.toSet()),
        Set.of(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)",
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_name)"));
  }

  @Test
  public void ownershipDeleteMirrorsToSchemaFields() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setOwnershipEnabled(true);
    Ownership ownership = testOwnership();

    stubSchemaMetadataStored(getTestSchemaMetadata());

    MCLItem ownershipDeleteItem =
        MCLItemImpl.builder()
            .metadataChangeLog(
                new MetadataChangeLog()
                    .setChangeType(ChangeType.DELETE)
                    .setEntityUrn(TEST_URN)
                    .setEntityType(DATASET_ENTITY_NAME)
                    .setAspectName(OWNERSHIP_ASPECT_NAME)
                    .setPreviousAspectValue(GenericRecordUtils.serializeAspect(ownership))
                    .setCreated(AuditStampUtils.createDefaultAuditStamp()))
            .build(retrieverContext.getAspectRetriever());

    List<MCPItem> testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY, List.of(ownershipDeleteItem), retrieverContext)
            .toList();

    assertEquals(testOutput.size(), 2);
    assertEquals(
        testOutput.stream().map(MCPItem::getAspectName).collect(Collectors.toSet()),
        Set.of(OWNERSHIP_ASPECT_NAME));
    assertTrue(testOutput.stream().allMatch(item -> item instanceof DeleteItemImpl));
    assertEquals(
        testOutput.stream().map(item -> item.getUrn().toString()).collect(Collectors.toSet()),
        Set.of(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)",
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_name)"));
  }

  @Test
  public void domainsDeleteFlagOffEmitsNoDeletes() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setDomainEnabled(false);
    Domains domains = testDomains();

    stubSchemaMetadataStored(getTestSchemaMetadata());

    MCLItem domainsDeleteItem =
        MCLItemImpl.builder()
            .metadataChangeLog(
                new MetadataChangeLog()
                    .setChangeType(ChangeType.DELETE)
                    .setEntityUrn(TEST_URN)
                    .setEntityType(DATASET_ENTITY_NAME)
                    .setAspectName(DOMAINS_ASPECT_NAME)
                    .setPreviousAspectValue(GenericRecordUtils.serializeAspect(domains))
                    .setCreated(AuditStampUtils.createDefaultAuditStamp()))
            .build(retrieverContext.getAspectRetriever());

    List<MCPItem> testOutput =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY, List.of(domainsDeleteItem), retrieverContext)
            .toList();

    assertEquals(testOutput.size(), 0);
  }

  @Test
  public void schemaMetadataUpsertMirrorsDomainsOnlyForNewFields() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setEntityChangeEventGeneratorRegistry(buildEntityChangeEventGeneratorRegistry());
    test.setDomainEnabled(true);
    SchemaMetadata previousSchema = getTestSchemaMetadataWithRemovedField(); // user_id only
    SchemaMetadata currentSchema = getTestSchemaMetadata(); // user_id + user_name
    Domains domains = testDomains();

    stubLatestAspects(Map.of(DOMAINS_ASPECT_NAME, new Aspect(domains.data())));

    ChangeItemImpl schemaMetadataChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_URN)
            .aspectName(SCHEMA_METADATA_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
            .recordTemplate(currentSchema)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    List<MCPItem> domainMirrors =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            schemaMetadataChangeItem,
                            previousSchema,
                            null,
                            retrieverContext.getAspectRetriever())),
                retrieverContext)
            .filter(item -> DOMAINS_ASPECT_NAME.equals(item.getAspectName()))
            .toList();

    assertEquals(domainMirrors.size(), 1);
    assertEquals(
        domainMirrors.get(0).getUrn().toString(),
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_name)");
  }

  @Test
  public void schemaMetadataUpsertSkipsDomainMirrorsForUnchangedFields() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setEntityChangeEventGeneratorRegistry(buildEntityChangeEventGeneratorRegistry());
    test.setDomainEnabled(true);
    SchemaMetadata schemaMetadata = getTestSchemaMetadata();
    Domains domains = testDomains();

    stubLatestAspects(Map.of(DOMAINS_ASPECT_NAME, new Aspect(domains.data())));

    ChangeItemImpl schemaMetadataChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_URN)
            .aspectName(SCHEMA_METADATA_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
            .recordTemplate(schemaMetadata)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    long domainMirrorCount =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            schemaMetadataChangeItem,
                            schemaMetadata,
                            null,
                            retrieverContext.getAspectRetriever())),
                retrieverContext)
            .filter(item -> DOMAINS_ASPECT_NAME.equals(item.getAspectName()))
            .count();

    assertEquals(domainMirrorCount, 0);
  }

  @Test
  public void schemaMetadataRestateMirrorsDomainsForAllFields() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setEntityChangeEventGeneratorRegistry(buildEntityChangeEventGeneratorRegistry());
    test.setDomainEnabled(true);
    SchemaMetadata schemaMetadata = getTestSchemaMetadata();
    Domains domains = testDomains();

    stubLatestAspects(Map.of(DOMAINS_ASPECT_NAME, new Aspect(domains.data())));

    MCLItem restateItem =
        MCLItemImpl.builder()
            .metadataChangeLog(
                new MetadataChangeLog()
                    .setEntityUrn(TEST_URN)
                    .setEntityType(DATASET_ENTITY_NAME)
                    .setChangeType(ChangeType.RESTATE)
                    .setAspectName(SCHEMA_METADATA_ASPECT_NAME)
                    .setAspect(GenericRecordUtils.serializeAspect(schemaMetadata))
                    .setPreviousAspectValue(GenericRecordUtils.serializeAspect(schemaMetadata))
                    .setCreated(AuditStampUtils.createDefaultAuditStamp()))
            .build(retrieverContext.getAspectRetriever());

    long domainMirrorCount =
        test.postMCPSideEffect(OperationFingerprint.EMPTY, List.of(restateItem), retrieverContext)
            .filter(item -> DOMAINS_ASPECT_NAME.equals(item.getAspectName()))
            .count();

    assertEquals(domainMirrorCount, 2);
  }

  @Test
  public void schemaMetadataSystemUpdateMirrorsDomainsForAllFields() {
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setEntityChangeEventGeneratorRegistry(buildEntityChangeEventGeneratorRegistry());
    test.setDomainEnabled(true);
    SchemaMetadata schemaMetadata = getTestSchemaMetadata();
    Domains domains = testDomains();

    SystemMetadata systemMetadata = SystemMetadataUtils.createDefaultSystemMetadata();
    systemMetadata.setProperties(
        new com.linkedin.data.template.StringMap(
            Map.of(
                com.linkedin.metadata.Constants.APP_SOURCE,
                com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE)));

    stubLatestAspects(Map.of(DOMAINS_ASPECT_NAME, new Aspect(domains.data())));

    ChangeItemImpl schemaMetadataChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_URN)
            .aspectName(SCHEMA_METADATA_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
            .recordTemplate(schemaMetadata)
            .systemMetadata(systemMetadata)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    long domainMirrorCount =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            schemaMetadataChangeItem,
                            schemaMetadata,
                            null,
                            retrieverContext.getAspectRetriever())),
                retrieverContext)
            .filter(item -> DOMAINS_ASPECT_NAME.equals(item.getAspectName()))
            .count();

    assertEquals(domainMirrorCount, 2);
  }

  @Test
  public void domainsCoBatchedWithUnchangedSchemaMetadataMirrorsAllFields() {
    // schemaMetadata path only mirrors new fields; domains must still fan out when co-batched.
    SchemaFieldSideEffect test = new SchemaFieldSideEffect();
    test.setConfig(TEST_PLUGIN_CONFIG);
    test.setEntityChangeEventGeneratorRegistry(buildEntityChangeEventGeneratorRegistry());
    test.setDomainEnabled(true);
    SchemaMetadata schemaMetadata = getTestSchemaMetadata();
    Domains domains = testDomains();

    stubEmptyAspectFetch();

    ChangeItemImpl schemaMetadataChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_URN)
            .aspectName(SCHEMA_METADATA_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
            .recordTemplate(schemaMetadata)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    ChangeItemImpl domainsChangeItem =
        ChangeItemImpl.builder()
            .urn(TEST_URN)
            .aspectName(DOMAINS_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectSpec(
                TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME).getAspectSpec(DOMAINS_ASPECT_NAME))
            .recordTemplate(domains)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    List<MCPItem> domainMirrors =
        test.postMCPSideEffect(
                OperationFingerprint.EMPTY,
                List.of(
                    MCLItemImpl.builder()
                        .build(
                            schemaMetadataChangeItem,
                            schemaMetadata,
                            null,
                            retrieverContext.getAspectRetriever()),
                    MCLItemImpl.builder()
                        .build(
                            domainsChangeItem, null, null, retrieverContext.getAspectRetriever())),
                retrieverContext)
            .filter(item -> DOMAINS_ASPECT_NAME.equals(item.getAspectName()))
            .toList();

    assertEquals(domainMirrors.size(), 2);
    assertEquals(
        domainMirrors.stream().map(item -> item.getUrn().toString()).collect(Collectors.toSet()),
        Set.of(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)",
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_name)"));
    assertTrue(domainMirrors.stream().allMatch(item -> item instanceof ChangeItemImpl));
    assertEquals(
        domainMirrors.stream()
            .map(item -> item.getAspect(Domains.class).getDomains())
            .collect(Collectors.toSet()),
        Set.of(domains.getDomains()));
  }

  private void stubSchemaMetadataStored(SchemaMetadata schemaMetadata) {
    stubLatestAspects(Map.of(SCHEMA_METADATA_ASPECT_NAME, new Aspect(schemaMetadata.data())));
  }

  private void stubLatestAspects(Map<String, Aspect> aspectsForTestUrn) {
    reset(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_REGISTRY);
    when(mockAspectRetriever.getLatestAspectObjects(
            any(OperationFingerprint.class), eq(Set.of(TEST_URN)), anySet()))
        .thenReturn(Map.of(TEST_URN, aspectsForTestUrn));
  }

  private void stubEmptyAspectFetch() {
    reset(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_REGISTRY);
    when(mockAspectRetriever.getLatestAspectObjects(
            any(OperationFingerprint.class), eq(Set.of(TEST_URN)), anySet()))
        .thenReturn(Map.of());
  }

  private static Domains testDomains() {
    return new Domains()
        .setDomains(new UrnArray(List.of(UrnUtils.getUrn("urn:li:domain:finance"))));
  }

  private static Ownership testOwnership() {
    return new Ownership()
        .setOwners(
            new OwnerArray(
                List.of(
                    new Owner()
                        .setOwner(UrnUtils.getUrn("urn:li:corpuser:jdoe"))
                        .setType(OwnershipType.TECHNICAL_OWNER))))
        .setLastModified(AuditStampUtils.createDefaultAuditStamp());
  }
}
