package com.linkedin.metadata.ingestion.validators;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.EntitySpec;
import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.ingestion.validation.ModifyIngestionSourceAuthValidator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ModifyIngestionSourceAuthValidatorTest {

  private static final Urn INGESTION_SOURCE_URN;
  private static final String INGESTION_SOURCE_URN_STRING = "urn:li:dataHubIngestionSource:test";

  private ModifyIngestionSourceAuthValidator validator;

  private EntityRegistry entityRegistry;
  @Mock private RetrieverContext mockRetrieverContext;

  @Mock private AuthorizationSession mockSession = Mockito.mock(AuthorizationSession.class);

  @Mock private AspectRetriever mockAspectRetriever;

  static {
    try {
      INGESTION_SOURCE_URN = Urn.createFromString(INGESTION_SOURCE_URN_STRING);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(ModifyIngestionSourceAuthValidator.class.getName())
          .enabled(true)
          .supportedOperations(
              List.of("DELETE", "PATCH", "UPSERT", "CREATE", "CREATE_ENTITY", "UPDATE", "RESTATE"))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName(INGESTION_SOURCE_ENTITY_NAME)
                      .aspectName(INGESTION_INFO_ASPECT_NAME)
                      .build()))
          .build();

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    entityRegistry = new TestEntityRegistry();
    validator = new ModifyIngestionSourceAuthValidator();
    validator.setConfig(TEST_PLUGIN_CONFIG);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
  }

  @Test
  public void testCreateAllow() {
    Set<String> allowedPrivileges = Set.of("MANAGE_INGESTION", "CREATE", "CREATE_ENTITY");
    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    when(mockSession.authorize(
            argThat(allowedPrivileges::contains),
            eq(new EntitySpec("dataHubIngestionSource", INGESTION_SOURCE_URN_STRING)),
            anyCollection()))
        .thenReturn(result);

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.CREATE)
                        .urn(INGESTION_SOURCE_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(INGESTION_SOURCE_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(INGESTION_SOURCE_URN.getEntityType())
                                .getAspectSpec(INGESTION_INFO_ASPECT_NAME))
                        .build(),
                    TestMCP.builder()
                        .changeType(ChangeType.CREATE_ENTITY)
                        .urn(INGESTION_SOURCE_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(INGESTION_SOURCE_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(INGESTION_SOURCE_URN.getEntityType())
                                .getAspectSpec(INGESTION_INFO_ASPECT_NAME))
                        .build()),
                mockRetrieverContext,
                mockSession)
            .count(),
        0,
        "Expected creating Ingestion source to be allowed");
  }

  @Test
  public void testCreateDenied() {
    Set<String> allowedPrivileges = Set.of("MANAGE_INGESTION", "CREATE", "CREATE_ENTITY");
    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.DENY);
    when(mockSession.authorize(
            argThat(allowedPrivileges::contains),
            eq(new EntitySpec("dataHubIngestionSource", INGESTION_SOURCE_URN_STRING)),
            anyCollection()))
        .thenReturn(result);

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.CREATE)
                        .urn(INGESTION_SOURCE_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(INGESTION_SOURCE_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(INGESTION_SOURCE_URN.getEntityType())
                                .getAspectSpec(INGESTION_INFO_ASPECT_NAME))
                        .build(),
                    TestMCP.builder()
                        .changeType(ChangeType.CREATE_ENTITY)
                        .urn(INGESTION_SOURCE_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(INGESTION_SOURCE_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(INGESTION_SOURCE_URN.getEntityType())
                                .getAspectSpec(INGESTION_INFO_ASPECT_NAME))
                        .build()),
                mockRetrieverContext,
                mockSession)
            .count(),
        2,
        "Expected creating Ingestion source to be denied");
  }

  @Test
  public void testEditAllow() {
    Set<String> allowedPrivileges = Set.of("MANAGE_INGESTION", "EDIT_ENTITY");
    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    when(mockSession.authorize(
            argThat(allowedPrivileges::contains),
            eq(new EntitySpec("dataHubIngestionSource", INGESTION_SOURCE_URN_STRING)),
            anyCollection()))
        .thenReturn(result);

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(INGESTION_SOURCE_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(INGESTION_SOURCE_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(INGESTION_SOURCE_URN.getEntityType())
                                .getAspectSpec(INGESTION_INFO_ASPECT_NAME))
                        .build(),
                    TestMCP.builder()
                        .changeType(ChangeType.UPDATE)
                        .urn(INGESTION_SOURCE_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(INGESTION_SOURCE_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(INGESTION_SOURCE_URN.getEntityType())
                                .getAspectSpec(INGESTION_INFO_ASPECT_NAME))
                        .build(),
                    TestMCP.builder()
                        .changeType(ChangeType.RESTATE)
                        .urn(INGESTION_SOURCE_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(INGESTION_SOURCE_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(INGESTION_SOURCE_URN.getEntityType())
                                .getAspectSpec(INGESTION_INFO_ASPECT_NAME))
                        .build()),
                mockRetrieverContext,
                mockSession)
            .count(),
        0,
        "Expected edits on the Ingestion source to be allowed");
  }

  @Test
  public void testEditDenied() {
    Set<String> allowedPrivileges = Set.of("MANAGE_INGESTION", "EDIT_ENTITY");
    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.DENY);
    when(mockSession.authorize(
            argThat(allowedPrivileges::contains),
            eq(new EntitySpec("dataHubIngestionSource", INGESTION_SOURCE_URN_STRING)),
            anyCollection()))
        .thenReturn(result);

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(INGESTION_SOURCE_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(INGESTION_SOURCE_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(INGESTION_SOURCE_URN.getEntityType())
                                .getAspectSpec(INGESTION_INFO_ASPECT_NAME))
                        .build(),
                    TestMCP.builder()
                        .changeType(ChangeType.UPDATE)
                        .urn(INGESTION_SOURCE_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(INGESTION_SOURCE_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(INGESTION_SOURCE_URN.getEntityType())
                                .getAspectSpec(INGESTION_INFO_ASPECT_NAME))
                        .build(),
                    TestMCP.builder()
                        .changeType(ChangeType.RESTATE)
                        .urn(INGESTION_SOURCE_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(INGESTION_SOURCE_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(INGESTION_SOURCE_URN.getEntityType())
                                .getAspectSpec(INGESTION_INFO_ASPECT_NAME))
                        .build()),
                mockRetrieverContext,
                mockSession)
            .count(),
        3,
        "Expected edits on the Ingestion source to be denied");
  }

  @Test
  public void testDeleteDenied() {
    Set<String> allowedPrivileges = Set.of("MANAGE_INGESTION", "DELETE_ENTITY");
    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.DENY);
    when(mockSession.authorize(
            argThat(allowedPrivileges::contains),
            eq(new EntitySpec("dataHubIngestionSource", INGESTION_SOURCE_URN_STRING)),
            anyCollection()))
        .thenReturn(result);

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(INGESTION_SOURCE_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(INGESTION_SOURCE_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(INGESTION_SOURCE_URN.getEntityType())
                                .getAspectSpec(INGESTION_INFO_ASPECT_NAME))
                        .build()),
                mockRetrieverContext,
                mockSession)
            .count(),
        1,
        "Expected deletes on the Ingestion source to be denied");
  }

  @Test
  public void testDeleteAllowed() {
    Set<String> allowedPrivileges = Set.of("MANAGE_INGESTION", "DELETE_ENTITY");
    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    when(mockSession.authorize(
            argThat(allowedPrivileges::contains),
            eq(new EntitySpec("dataHubIngestionSource", INGESTION_SOURCE_URN_STRING)),
            anyCollection()))
        .thenReturn(result);

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(INGESTION_SOURCE_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(INGESTION_SOURCE_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(INGESTION_SOURCE_URN.getEntityType())
                                .getAspectSpec(INGESTION_INFO_ASPECT_NAME))
                        .build()),
                mockRetrieverContext,
                mockSession)
            .count(),
        0,
        "Expected deletes on the Ingestion source to be allowed");
  }

  @Test
  public void testNullSession() {
    // Test when session is null
    final DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo();
    ingestionSourceInfo.setName("Test Source");

    BatchItem testItem =
        TestMCP.builder()
            .changeType(ChangeType.CREATE)
            .urn(INGESTION_SOURCE_URN)
            .entitySpec(entityRegistry.getEntitySpec(INGESTION_SOURCE_ENTITY_NAME))
            .aspectSpec(
                entityRegistry
                    .getEntitySpec(INGESTION_SOURCE_ENTITY_NAME)
                    .getAspectSpec(INGESTION_INFO_ASPECT_NAME))
            .recordTemplate(ingestionSourceInfo)
            .build();

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY, Set.of(testItem), mockRetrieverContext, null)
            .count(),
        0,
        "Expected no exceptions when session is null");
  }

  @Test
  public void testEmptyBatchItems() {
    // Test with empty collection of batch items
    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                Collections.emptySet(),
                mockRetrieverContext,
                mockSession)
            .count(),
        0,
        "Expected no exceptions for empty batch items");
  }

  @Test
  public void testConfigGetterSetter() {
    // Test config getter and setter
    AspectPluginConfig newConfig =
        AspectPluginConfig.builder()
            .className("TestClass")
            .enabled(false)
            .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
            .build();

    validator.setConfig(newConfig);
    assertEquals(validator.getConfig(), newConfig);

    // Test chaining
    ModifyIngestionSourceAuthValidator chainedValidator =
        new ModifyIngestionSourceAuthValidator().setConfig(TEST_PLUGIN_CONFIG);
    assertEquals(chainedValidator.getConfig(), TEST_PLUGIN_CONFIG);
  }

  // A scheduled source is run by the scheduler as the system actor, so EDIT_ENTITY alone must not
  // be enough to leave a schedule on it.
  @Test
  public void testScheduledEditDeniedWithoutExecute() {
    grant("EDIT_ENTITY");

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                Set.of(
                    infoItem(ChangeType.UPSERT, sourceInfo(true)),
                    infoItem(ChangeType.UPDATE, sourceInfo(true)),
                    infoItem(ChangeType.RESTATE, sourceInfo(true))),
                mockRetrieverContext,
                mockSession)
            .count(),
        3,
        "Expected scheduling the Ingestion source without execute permission to be denied");
  }

  @Test
  public void testScheduledEditAllowedWithExecute() {
    grant("EDIT_ENTITY", "EXECUTE_ENTITY");

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                Set.of(
                    infoItem(ChangeType.UPSERT, sourceInfo(true)),
                    infoItem(ChangeType.UPDATE, sourceInfo(true)),
                    infoItem(ChangeType.RESTATE, sourceInfo(true))),
                mockRetrieverContext,
                mockSession)
            .count(),
        0,
        "Expected scheduling the Ingestion source with execute permission to be allowed");
  }

  @Test
  public void testScheduledWritesAllowedWithManageIngestion() {
    grant("MANAGE_INGESTION");

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                Set.of(
                    infoItem(ChangeType.CREATE, sourceInfo(true)),
                    infoItem(ChangeType.UPSERT, sourceInfo(true))),
                mockRetrieverContext,
                mockSession)
            .count(),
        0,
        "Expected MANAGE_INGESTION to allow scheduling the Ingestion source");
  }

  // Renaming a source or removing its schedule stays an edit.
  @Test
  public void testUnscheduledEditAllowedWithEditOnly() {
    grant("EDIT_ENTITY");

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                Set.of(infoItem(ChangeType.UPSERT, sourceInfo(false))),
                mockRetrieverContext,
                mockSession)
            .count(),
        0,
        "Expected edits leaving no schedule on the Ingestion source to be allowed");
    Mockito.verify(mockSession, Mockito.never())
        .authorize(eq("EXECUTE_ENTITY"), any(), anyCollection());
  }

  @Test
  public void testDeleteDoesNotRequireExecute() {
    grant("DELETE_ENTITY");

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                Set.of(infoItem(ChangeType.DELETE, sourceInfo(true))),
                mockRetrieverContext,
                mockSession)
            .count(),
        0,
        "Expected deleting a scheduled Ingestion source to need delete permission only");
    Mockito.verify(mockSession, Mockito.never())
        .authorize(eq("EXECUTE_ENTITY"), any(), anyCollection());
  }

  @Test
  public void testMixedBatchDeniesOnlyScheduledItem() {
    grant("EDIT_ENTITY");
    Urn otherUrn = UrnUtils.getUrn("urn:li:dataHubIngestionSource:other");
    grantOn(otherUrn, "EDIT_ENTITY");

    List<String> denied =
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                Set.of(
                    infoItem(ChangeType.UPSERT, sourceInfo(false)),
                    infoItem(otherUrn, ChangeType.UPSERT, sourceInfo(true))),
                mockRetrieverContext,
                mockSession)
            .map(e -> e.getAspectGroup().getKey().toString())
            .toList();

    assertEquals(denied, List.of(otherUrn.toString()));
  }

  // dataHubIngestionSourceInfo has no patch template, so a generic JSON patch is the only PATCH
  // that applies; whether it arrives as a PatchItemImpl or a ProposedItem, the schedule it leaves
  // behind must be checked.
  @DataProvider(name = "patchItemShapes")
  public Object[][] patchItemShapes() {
    return new Object[][] {{true}, {false}};
  }

  @Test(dataProvider = "patchItemShapes")
  public void testPatchAddingScheduleDeniedWithoutExecute(boolean alternateValidation) {
    grant("EDIT_ENTITY");
    stubStored(sourceInfo(false));

    BatchItem item =
        patchItem(
            alternateValidation,
            true,
            addOp("/schedule", Map.of("interval", "0 * * * *", "timezone", "UTC")));

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY, Set.of(item), mockRetrieverContext, mockSession)
            .count(),
        1);
    // The denial must come from the schedule check, not from failing to resolve the patch.
    Mockito.verify(mockSession, Mockito.atLeastOnce())
        .authorize(
            eq("EXECUTE_ENTITY"),
            eq(new EntitySpec("dataHubIngestionSource", INGESTION_SOURCE_URN_STRING)),
            anyCollection());
  }

  // A generic PATCH can create the aspect from scratch; the schedule it carries must still be
  // checked. Required fields are included so the patch resolves rather than failing as invalid.
  @Test(dataProvider = "patchItemShapes")
  public void testPatchCreatingScheduledSourceDeniedWithoutExecute(boolean alternateValidation) {
    grant("EDIT_ENTITY");

    BatchItem item =
        patchItem(
            alternateValidation,
            true,
            addOp("/name", "Test Source"),
            addOp("/type", "mysql"),
            addOp("/config", Map.of("recipe", "{}")),
            addOp("/schedule", Map.of("interval", "0 * * * *", "timezone", "UTC")));

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY, Set.of(item), mockRetrieverContext, mockSession)
            .count(),
        1);
    // The denial must come from the schedule check, not from failing to resolve the patch.
    Mockito.verify(mockSession, Mockito.atLeastOnce())
        .authorize(
            eq("EXECUTE_ENTITY"),
            eq(new EntitySpec("dataHubIngestionSource", INGESTION_SOURCE_URN_STRING)),
            anyCollection());
  }

  // Changing the recipe of an already scheduled source changes what the scheduler runs.
  @Test(dataProvider = "patchItemShapes")
  public void testPatchOnScheduledSourceDeniedWithoutExecute(boolean alternateValidation) {
    grant("EDIT_ENTITY");
    stubStored(sourceInfo(true));

    BatchItem item =
        patchItem(alternateValidation, true, addOp("/config/recipe", "{\"source\":{}}"));

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY, Set.of(item), mockRetrieverContext, mockSession)
            .count(),
        1);
    Mockito.verify(mockSession, Mockito.atLeastOnce())
        .authorize(
            eq("EXECUTE_ENTITY"),
            eq(new EntitySpec("dataHubIngestionSource", INGESTION_SOURCE_URN_STRING)),
            anyCollection());
  }

  @Test(dataProvider = "patchItemShapes")
  public void testPatchOnUnscheduledSourceAllowedWithEditOnly(boolean alternateValidation) {
    grant("EDIT_ENTITY");
    stubStored(sourceInfo(false));

    BatchItem item = patchItem(alternateValidation, true, addOp("/name", "renamed"));

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY, Set.of(item), mockRetrieverContext, mockSession)
            .count(),
        0);
  }

  @Test(dataProvider = "patchItemShapes")
  public void testPatchRemovingScheduleAllowedWithEditOnly(boolean alternateValidation) {
    grant("EDIT_ENTITY");
    stubStored(sourceInfo(true));

    GenericJsonPatch.PatchOp remove = new GenericJsonPatch.PatchOp();
    remove.setOp("remove");
    remove.setPath("/schedule");
    BatchItem item = patchItem(alternateValidation, true, remove);

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY, Set.of(item), mockRetrieverContext, mockSession)
            .count(),
        0);
  }

  // A PATCH applies on top of earlier items in the same batch, not the stored aspect: removing the
  // schedule and then renaming in one batch leaves the source unscheduled.
  @Test(dataProvider = "patchItemShapes")
  public void testPatchResolvesAgainstEarlierItemInBatch(boolean alternateValidation) {
    grant("EDIT_ENTITY");
    stubStored(sourceInfo(true));

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                List.of(
                    infoItem(ChangeType.UPSERT, sourceInfo(false)),
                    patchItem(alternateValidation, true, addOp("/name", "renamed"))),
                mockRetrieverContext,
                mockSession)
            .count(),
        0);
  }

  // The security direction of in-batch chaining: a PATCH that puts the schedule back after an
  // earlier item removed it must still need EXECUTE.
  @Test(dataProvider = "patchItemShapes")
  public void testPatchReschedulingAfterEarlierItemInBatchDenied(boolean alternateValidation) {
    grant("EDIT_ENTITY");
    stubStored(sourceInfo(true));

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                List.of(
                    infoItem(ChangeType.UPSERT, sourceInfo(false)),
                    patchItem(
                        alternateValidation,
                        true,
                        addOp("/schedule", Map.of("interval", "0 * * * *", "timezone", "UTC")))),
                mockRetrieverContext,
                mockSession)
            .count(),
        1);
  }

  // A denied item is not written, so it must not become the base for later PATCHes.
  @Test(dataProvider = "patchItemShapes")
  public void testDeniedItemNotUsedAsBatchBase(boolean alternateValidation) {
    grant("EDIT_ENTITY");
    stubStored(sourceInfo(false));

    List<String> denied =
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                List.of(
                    infoItem(ChangeType.UPSERT, sourceInfo(true)),
                    patchItem(alternateValidation, true, addOp("/name", "renamed"))),
                mockRetrieverContext,
                mockSession)
            .map(e -> e.getChangeType().toString())
            .toList();

    assertEquals(denied, List.of(ChangeType.UPSERT.toString()));
  }

  // The proposal check reads the stored aspect before the write lock; pre-commit checks the aspect
  // actually being written, which may carry a schedule another writer added in between.
  @Test
  public void testPreCommitScheduledDeniedWithoutExecute() {
    grant("EDIT_ENTITY");

    assertEquals(
        validator
            .validatePreCommit(
                OperationFingerprint.EMPTY,
                List.<ChangeMCP>of(infoItem(ChangeType.UPSERT, sourceInfo(true))),
                mockRetrieverContext,
                mockSession)
            .count(),
        1);
  }

  @Test
  public void testPreCommitScheduledAllowedWithExecute() {
    grant("EDIT_ENTITY", "EXECUTE_ENTITY");

    assertEquals(
        validator
            .validatePreCommit(
                OperationFingerprint.EMPTY,
                List.<ChangeMCP>of(infoItem(ChangeType.UPSERT, sourceInfo(true))),
                mockRetrieverContext,
                mockSession)
            .count(),
        0);
  }

  @Test
  public void testPreCommitSkipsUnscheduledDeleteAndNullSession() {
    grant("EDIT_ENTITY");

    assertEquals(
        validator
            .validatePreCommit(
                OperationFingerprint.EMPTY,
                List.<ChangeMCP>of(
                    infoItem(ChangeType.UPSERT, sourceInfo(false)),
                    infoItem(ChangeType.DELETE, sourceInfo(true))),
                mockRetrieverContext,
                mockSession)
            .count(),
        0);
    assertEquals(
        validator
            .validatePreCommit(
                OperationFingerprint.EMPTY,
                List.<ChangeMCP>of(infoItem(ChangeType.UPSERT, sourceInfo(true))),
                mockRetrieverContext,
                null)
            .count(),
        0);
  }

  @Test(dataProvider = "patchItemShapes")
  public void testUnresolvablePatchDenied(boolean alternateValidation) {
    grant("EDIT_ENTITY", "EXECUTE_ENTITY");

    BatchItem item = patchItem(alternateValidation, false, addOp("/name", "renamed"));

    assertEquals(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY, Set.of(item), mockRetrieverContext, mockSession)
            .count(),
        1);
  }

  private void grant(String... privileges) {
    grantOn(INGESTION_SOURCE_URN, privileges);
  }

  private void grantOn(Urn urn, String... privileges) {
    Set<String> granted = Set.of(privileges);
    AuthorizationResult allow = Mockito.mock(AuthorizationResult.class);
    Mockito.when(allow.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    AuthorizationResult deny = Mockito.mock(AuthorizationResult.class);
    Mockito.when(deny.getType()).thenReturn(AuthorizationResult.Type.DENY);
    EntitySpec spec = new EntitySpec(INGESTION_SOURCE_ENTITY_NAME, urn.toString());
    when(mockSession.authorize(any(), eq(spec), anyCollection()))
        .thenAnswer(inv -> granted.contains(inv.<String>getArgument(0)) ? allow : deny);
  }

  private void stubStored(DataHubIngestionSourceInfo stored) {
    when(mockAspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(INGESTION_SOURCE_URN)), eq(Set.of(INGESTION_INFO_ASPECT_NAME))))
        .thenReturn(
            Map.of(
                INGESTION_SOURCE_URN,
                Map.of(INGESTION_INFO_ASPECT_NAME, new Aspect(stored.data()))));
  }

  private static DataHubIngestionSourceInfo sourceInfo(boolean scheduled) {
    DataHubIngestionSourceInfo info =
        new DataHubIngestionSourceInfo()
            .setName("Test Source")
            .setType("mysql")
            .setConfig(new DataHubIngestionSourceConfig().setRecipe("{}"));
    if (scheduled) {
      info.setSchedule(
          new DataHubIngestionSourceSchedule().setInterval("0 * * * *").setTimezone("UTC"));
    }
    return info;
  }

  private TestMCP infoItem(ChangeType changeType, DataHubIngestionSourceInfo info) {
    return infoItem(INGESTION_SOURCE_URN, changeType, info);
  }

  private TestMCP infoItem(Urn urn, ChangeType changeType, DataHubIngestionSourceInfo info) {
    return TestMCP.builder()
        .changeType(changeType)
        .urn(urn)
        .entitySpec(entityRegistry.getEntitySpec(INGESTION_SOURCE_ENTITY_NAME))
        .aspectSpec(
            entityRegistry
                .getEntitySpec(INGESTION_SOURCE_ENTITY_NAME)
                .getAspectSpec(INGESTION_INFO_ASPECT_NAME))
        .recordTemplate(info)
        .build();
  }

  private BatchItem patchItem(
      boolean alternateValidation, boolean forceGenericPatch, GenericJsonPatch.PatchOp... ops) {
    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setEntityUrn(INGESTION_SOURCE_URN)
            .setEntityType(INGESTION_SOURCE_ENTITY_NAME)
            .setAspectName(INGESTION_INFO_ASPECT_NAME)
            .setChangeType(ChangeType.PATCH)
            .setAspect(
                GenericRecordUtils.serializePatch(
                    GenericJsonPatch.builder()
                        .patch(List.of(ops))
                        .forceGenericPatch(forceGenericPatch)
                        .build(),
                    new ObjectMapper()));
    return alternateValidation
        ? ProposedItem.builder()
            .build(mcp, AuditStampUtils.createDefaultAuditStamp(), entityRegistry)
        : PatchItemImpl.builder()
            .build(mcp, AuditStampUtils.createDefaultAuditStamp(), entityRegistry);
  }

  private static GenericJsonPatch.PatchOp addOp(String path, Object value) {
    GenericJsonPatch.PatchOp op = new GenericJsonPatch.PatchOp();
    op.setOp("add");
    op.setPath(path);
    op.setValue(value);
    return op;
  }
}
