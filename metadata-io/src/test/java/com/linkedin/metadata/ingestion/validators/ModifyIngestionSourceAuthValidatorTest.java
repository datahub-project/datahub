package com.linkedin.metadata.ingestion.validators;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.ingestion.validation.ModifyIngestionSourceAuthValidator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
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
        validator.validateProposed(Set.of(testItem), mockRetrieverContext, null).count(),
        0,
        "Expected no exceptions when session is null");
  }

  @Test
  public void testEmptyBatchItems() {
    // Test with empty collection of batch items
    assertEquals(
        validator
            .validateProposed(Collections.emptySet(), mockRetrieverContext, mockSession)
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
}
