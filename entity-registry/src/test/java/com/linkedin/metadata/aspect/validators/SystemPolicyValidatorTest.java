package com.linkedin.metadata.aspect.validators;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.validation.SystemPolicyValidator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.List;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SystemPolicyValidatorTest {
  private static final Urn NON_SYSTEM_POLICY_URN = UrnUtils.getUrn("urn:li:dataHubPolicy:custom-policy");
  private static final Urn SYSTEM_ACTOR_URN = UrnUtils.getUrn(SYSTEM_ACTOR);;
  private static final Urn NON_SYSTEM_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:user");

  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(SystemPolicyValidator.class.getName())
          .enabled(true)
          .supportedOperations(List.of("DELETE", "UPSERT", "UPDATE", "PATCH"))
          .supportedEntityAspectNames(
              List.of(new AspectPluginConfig.EntityAspectName("dataHubPolicy", "*")))
          .build();

  @Mock private RetrieverContext mockRetrieverContext;

  @Mock private AspectRetriever mockAspectRetriever;

  private EntityRegistry entityRegistry;

  private SystemPolicyValidator validator;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    entityRegistry = new TestEntityRegistry();
    validator = new SystemPolicyValidator();
    validator.setConfig(TEST_PLUGIN_CONFIG);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
  }

  @Test
  public void testSystemPolicyDeleteDenied() {
    // Test deletion of SYSTEM_POLICY_ZERO is denied
    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(SYSTEM_POLICY_ZERO)
                        .entitySpec(entityRegistry.getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(new DataHubPolicyInfo()
                            .setActors(new DataHubActorFilter())
                            .setEditable(true)
                            .setDescription("")
                            .setDisplayName("")
                            .setLastUpdatedTimestamp(123L)
                            .setPrivileges(new StringArray())
                            .setState("ACTIVE")
                            .setType("")
                        )
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "Expected deletion of system policy zero to be denied");

    // Test deletion of SYSTEM_POLICY_ONE is denied
    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(SYSTEM_POLICY_ONE)
                        .entitySpec(entityRegistry.getEntitySpec(SYSTEM_POLICY_ONE.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(SYSTEM_POLICY_ONE.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(new DataHubPolicyInfo()
                            .setActors(new DataHubActorFilter())
                            .setEditable(true)
                            .setDescription("")
                            .setDisplayName("")
                            .setLastUpdatedTimestamp(123L)
                            .setPrivileges(new StringArray())
                            .setState("ACTIVE")
                            .setType(""))
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "Expected deletion of system policy one to be denied");
  }

  @Test
  public void testNonSystemPolicyDeleteAllowed() {
    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(NON_SYSTEM_POLICY_URN)
                        .entitySpec(entityRegistry.getEntitySpec(NON_SYSTEM_POLICY_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(NON_SYSTEM_POLICY_URN.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(new DataHubPolicyInfo()
                            .setActors(new DataHubActorFilter())
                            .setEditable(true)
                            .setDescription("")
                            .setDisplayName("")
                            .setLastUpdatedTimestamp(123L)
                            .setPrivileges(new StringArray())
                            .setState("ACTIVE")
                            .setType(""))
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected deletion of non-system policy to be allowed");
  }

  @Test
  public void testNonEditablePolicyModifyByNonSystemActorDenied() {
    final DataHubPolicyInfo policyInfo = new DataHubPolicyInfo()
        .setActors(new DataHubActorFilter())
        .setEditable(false)
        .setDescription("")
        .setDisplayName("")
        .setLastUpdatedTimestamp(123L)
        .setPrivileges(new StringArray())
        .setState("ACTIVE")
        .setType("");

    when(mockAspectRetriever.getLatestAspectObject(SYSTEM_POLICY_ZERO, DATAHUB_POLICY_INFO_ASPECT_NAME))
        .thenReturn(new Aspect(policyInfo.data()));

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(NON_SYSTEM_ACTOR_URN);

    // Test UPSERT operation
    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(SYSTEM_POLICY_ZERO)
                        .entitySpec(entityRegistry.getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(policyInfo)
                        .auditStamp(auditStamp)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "Expected UPSERT of non-editable policy by non-system actor to be denied");

    // Test UPDATE operation
    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPDATE)
                        .urn(SYSTEM_POLICY_ZERO)
                        .entitySpec(entityRegistry.getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(policyInfo)
                        .auditStamp(auditStamp)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "Expected UPDATE of non-editable policy by non-system actor to be denied");

    // Test PATCH operation
    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.PATCH)
                        .urn(SYSTEM_POLICY_ZERO)
                        .entitySpec(entityRegistry.getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(policyInfo)
                        .auditStamp(auditStamp)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "Expected PATCH of non-editable policy by non-system actor to be denied");
  }

  @Test
  public void testNonEditablePolicyModifyBySystemActorAllowed() {
    final DataHubPolicyInfo policyInfo = new DataHubPolicyInfo()
        .setActors(new DataHubActorFilter())
        .setEditable(false)
        .setDescription("")
        .setDisplayName("")
        .setLastUpdatedTimestamp(123L)
        .setPrivileges(new StringArray())
        .setState("ACTIVE")
        .setType("");

    when(mockAspectRetriever.getLatestAspectObject(SYSTEM_POLICY_ZERO, DATAHUB_POLICY_INFO_ASPECT_NAME))
        .thenReturn(new Aspect(policyInfo.data()));

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(SYSTEM_ACTOR_URN);

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(SYSTEM_POLICY_ZERO)
                        .entitySpec(entityRegistry.getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(policyInfo)
                        .auditStamp(auditStamp)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected UPSERT of non-editable policy by system actor to be allowed");
  }

  @Test
  public void testEditablePolicyModifyAllowed() {
    final DataHubPolicyInfo policyInfo = new DataHubPolicyInfo()
        .setActors(new DataHubActorFilter())
        .setEditable(true)
        .setDescription("")
        .setDisplayName("")
        .setLastUpdatedTimestamp(123L)
        .setPrivileges(new StringArray())
        .setState("ACTIVE")
        .setType("");

    when(mockAspectRetriever.getLatestAspectObject(SYSTEM_POLICY_ZERO, DATAHUB_POLICY_INFO_ASPECT_NAME))
        .thenReturn(new Aspect(policyInfo.data()));

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(NON_SYSTEM_ACTOR_URN);

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(SYSTEM_POLICY_ZERO)
                        .entitySpec(entityRegistry.getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(policyInfo)
                        .auditStamp(auditStamp)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected UPSERT of editable policy to be allowed");
  }

  @Test
  public void testModifyWithoutAuditStampDenied() {
    final DataHubPolicyInfo policyInfo = new DataHubPolicyInfo()
        .setActors(new DataHubActorFilter())
        .setEditable(false)
        .setDescription("")
        .setDisplayName("")
        .setLastUpdatedTimestamp(123L)
        .setPrivileges(new StringArray())
        .setState("ACTIVE")
        .setType("");

    when(mockAspectRetriever.getLatestAspectObject(SYSTEM_POLICY_ZERO, DATAHUB_POLICY_INFO_ASPECT_NAME))
        .thenReturn(new Aspect(policyInfo.data()));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(SYSTEM_POLICY_ZERO)
                        .entitySpec(entityRegistry.getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(policyInfo)
                        // No audit stamp set
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        1,
        "Expected UPSERT of non-editable policy without audit stamp to be denied");
  }

  @Test
  public void testModifyWhenNoPolicyInfoAspectExists() {
    // When no existing policy info aspect exists, modification should be allowed
    when(mockAspectRetriever.getLatestAspectObject(SYSTEM_POLICY_ZERO, DATAHUB_POLICY_INFO_ASPECT_NAME))
        .thenReturn(null);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(NON_SYSTEM_ACTOR_URN);

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(SYSTEM_POLICY_ZERO)
                        .entitySpec(entityRegistry.getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(SYSTEM_POLICY_ZERO.getEntityType())
                                .getAspectSpec(DATAHUB_POLICY_INFO_ASPECT_NAME))
                        .recordTemplate(new DataHubPolicyInfo()
                            .setActors(new DataHubActorFilter())
                            .setEditable(true)
                            .setDescription("")
                            .setDisplayName("")
                            .setLastUpdatedTimestamp(123L)
                            .setPrivileges(new StringArray())
                            .setState("ACTIVE")
                            .setType(""))
                        .auditStamp(auditStamp)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected UPSERT when no existing policy info aspect exists to be allowed");
  }
}