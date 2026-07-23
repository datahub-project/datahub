package com.linkedin.metadata.aspect.validators;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.PatchMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.validation.CorpUserPrivilegedFlagsValidator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.test.metadata.aspect.batch.TestPatchMCP;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CorpUserPrivilegedFlagsValidatorTest {

  private static final Urn TARGET_USER_URN = UrnUtils.getUrn("urn:li:corpuser:target");
  private static final Urn REGULAR_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:regular");
  private static final Urn SYSTEM_ACTOR_USER_URN = UrnUtils.getUrn("urn:li:corpuser:datahub");
  private static final Urn ADMIN_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:admin");
  private static final Urn SUPPORT_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:support");
  private static final Urn INTRINSIC_SYSTEM_ACTOR_URN = UrnUtils.getUrn(SYSTEM_ACTOR);

  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(CorpUserPrivilegedFlagsValidator.class.getName())
          .enabled(true)
          .supportedOperations(List.of("UPSERT", "UPDATE", "CREATE", "CREATE_ENTITY", "PATCH"))
          .supportedEntityAspectNames(
              List.of(
                  new AspectPluginConfig.EntityAspectName("corpuser", CORP_USER_INFO_ASPECT_NAME)))
          .build();

  @Mock private RetrieverContext mockRetrieverContext;
  @Mock private AspectRetriever mockAspectRetriever;

  private EntityRegistry entityRegistry;
  private CorpUserPrivilegedFlagsValidator validator;
  private Map<Urn, CorpUserInfo> corpUserInfoByUrn;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    entityRegistry = new TestEntityRegistry();
    validator = new CorpUserPrivilegedFlagsValidator();
    validator.setConfig(TEST_PLUGIN_CONFIG);
    corpUserInfoByUrn = new HashMap<>();
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    when(mockAspectRetriever.getLatestAspectObject(
            any(), any(Urn.class), eq(CORP_USER_INFO_ASPECT_NAME)))
        .thenAnswer(
            invocation -> {
              Urn urn = invocation.getArgument(1);
              CorpUserInfo info = corpUserInfoByUrn.get(urn);
              return info != null ? new Aspect(info.data()) : null;
            });
  }

  @Test
  public void testRegularUserCannotSetSystemTrue() {
    corpUserInfoByUrn.put(REGULAR_ACTOR_URN, corpUserInfo(false, false));

    CorpUserInfo proposed = corpUserInfo(true, false);
    long violations = validate(proposed, null, operationContext(REGULAR_ACTOR_URN), null).count();
    assertEquals(violations, 1);
  }

  @Test
  public void testSystemUserCanSetSystemTrueOnAnotherUser() {
    corpUserInfoByUrn.put(SYSTEM_ACTOR_USER_URN, corpUserInfo(true, false));

    CorpUserInfo proposed = corpUserInfo(true, false);
    long violations =
        validate(proposed, null, operationContext(SYSTEM_ACTOR_USER_URN), null).count();
    assertEquals(violations, 0);
  }

  @Test
  public void testSystemTrueNoOpWhenAlreadySystem() {
    corpUserInfoByUrn.put(REGULAR_ACTOR_URN, corpUserInfo(false, false));
    CorpUserInfo current = corpUserInfo(true, false);
    CorpUserInfo proposed = corpUserInfo(true, false);

    long violations =
        validate(proposed, current, operationContext(REGULAR_ACTOR_URN), null).count();
    assertEquals(violations, 0);
  }

  @Test
  public void testSettingSystemFalseAllowed() {
    corpUserInfoByUrn.put(REGULAR_ACTOR_URN, corpUserInfo(false, false));

    CorpUserInfo proposed = corpUserInfo(false, false);
    long violations =
        validate(proposed, corpUserInfo(true, false), operationContext(REGULAR_ACTOR_URN), null)
            .count();
    assertEquals(violations, 0);
  }

  @Test
  public void testSystemUpdateSourceBypass() {
    CorpUserInfo proposed = corpUserInfo(true, false);
    SystemMetadata systemMetadata = systemMetadataWithSource(SYSTEM_UPDATE_SOURCE);

    long violations =
        validate(proposed, null, operationContext(REGULAR_ACTOR_URN), systemMetadata).count();
    assertEquals(violations, 0);
  }

  @Test
  public void testSystemActorAuditStampBypass() {
    CorpUserInfo proposed = corpUserInfo(true, false);
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    long violations =
        validate(proposed, null, operationContext(REGULAR_ACTOR_URN), null, auditStamp).count();
    assertEquals(violations, 0);
  }

  @Test
  public void testActorWithoutCorpUserInfoRejected() {
    CorpUserInfo proposed = corpUserInfo(true, false);
    long violations = validate(proposed, null, operationContext(REGULAR_ACTOR_URN), null).count();
    assertEquals(violations, 1);
  }

  @Test
  public void testRegularUserCannotSetSupportTrue() {
    corpUserInfoByUrn.put(REGULAR_ACTOR_URN, corpUserInfo(false, false));

    CorpUserInfo proposed = corpUserInfo(false, true);
    long violations = validate(proposed, null, operationContext(REGULAR_ACTOR_URN), null).count();
    assertEquals(violations, 1);
  }

  @Test
  public void testSupportUserCanSetSupportTrueOnAnotherUser() {
    corpUserInfoByUrn.put(SUPPORT_ACTOR_URN, corpUserInfo(false, true));

    CorpUserInfo proposed = corpUserInfo(false, true);
    long violations = validate(proposed, null, operationContext(SUPPORT_ACTOR_URN), null).count();
    assertEquals(violations, 0);
  }

  @Test
  public void testSystemUserCanSetSupportTrue() {
    corpUserInfoByUrn.put(SYSTEM_ACTOR_USER_URN, corpUserInfo(true, false));

    CorpUserInfo proposed = corpUserInfo(false, true);
    long violations =
        validate(proposed, null, operationContext(SYSTEM_ACTOR_USER_URN), null).count();
    assertEquals(violations, 0);
  }

  @Test
  public void testSupportTrueNoOpWhenAlreadySupport() {
    corpUserInfoByUrn.put(REGULAR_ACTOR_URN, corpUserInfo(false, false));
    CorpUserInfo current = corpUserInfo(false, true);
    CorpUserInfo proposed = corpUserInfo(false, true);

    long violations =
        validate(proposed, current, operationContext(REGULAR_ACTOR_URN), null).count();
    assertEquals(violations, 0);
  }

  @Test
  public void testSettingSupportFalseAllowed() {
    corpUserInfoByUrn.put(REGULAR_ACTOR_URN, corpUserInfo(false, false));

    CorpUserInfo proposed = corpUserInfo(false, false);
    long violations =
        validate(proposed, corpUserInfo(false, true), operationContext(REGULAR_ACTOR_URN), null)
            .count();
    assertEquals(violations, 0);
  }

  @Test
  public void testPatchSystemEscalationRejectedForRegularUser() {
    corpUserInfoByUrn.put(REGULAR_ACTOR_URN, corpUserInfo(false, false));
    corpUserInfoByUrn.put(TARGET_USER_URN, corpUserInfo(false, false));

    ChangeMCP patchedChange =
        TestMCP.builder()
            .changeType(ChangeType.PATCH)
            .urn(TARGET_USER_URN)
            .entitySpec(entityRegistry.getEntitySpec(TARGET_USER_URN.getEntityType()))
            .aspectSpec(
                entityRegistry
                    .getEntitySpec(TARGET_USER_URN.getEntityType())
                    .getAspectSpec(CORP_USER_INFO_ASPECT_NAME))
            .recordTemplate(corpUserInfo(true, false))
            .build();

    PatchMCP patchItem = mock(PatchMCP.class);
    when(patchItem.getChangeType()).thenReturn(ChangeType.PATCH);
    when(patchItem.getAspectName()).thenReturn(CORP_USER_INFO_ASPECT_NAME);
    when(patchItem.getUrn()).thenReturn(TARGET_USER_URN);
    when(patchItem.getSystemMetadata()).thenReturn(null);
    when(patchItem.getAuditStamp()).thenReturn(null);
    when(patchItem.applyPatch(any(), any())).thenReturn(patchedChange);

    long violations =
        validator
            .validateProposed(
                operationContext(REGULAR_ACTOR_URN), Set.of(patchItem), mockRetrieverContext, null)
            .count();
    assertEquals(violations, 1);
  }

  /**
   * With alternate MCP validation (the quickstart/docker default) a patch reaches the proposed hook
   * as a raw proposal item, not a PatchMCP — the escalation must still be detected from the patch's
   * own add/replace values.
   */
  @Test
  public void testPatchSystemEscalationRejectedViaProposedItemShape() {
    corpUserInfoByUrn.put(REGULAR_ACTOR_URN, corpUserInfo(false, false));
    corpUserInfoByUrn.put(TARGET_USER_URN, corpUserInfo(false, false));

    String serialized = "{\"patch\":[{\"op\":\"add\",\"path\":\"/system\",\"value\":true}]}";

    long violations =
        validator
            .validateProposed(
                operationContext(REGULAR_ACTOR_URN),
                Set.of(
                    TestPatchMCP.ofProposed(
                        TARGET_USER_URN, CORP_USER_INFO_ASPECT_NAME, serialized)),
                mockRetrieverContext,
                null)
            .count();
    assertEquals(violations, 1);
  }

  @Test
  public void testStaticFlagHelpers() {
    assertEquals(CorpUserPrivilegedFlagsValidator.isSystemUser(null), false);
    assertEquals(CorpUserPrivilegedFlagsValidator.isSupportUser(null), false);
    assertEquals(CorpUserPrivilegedFlagsValidator.isSystemUser(corpUserInfo(true, false)), true);
    assertEquals(CorpUserPrivilegedFlagsValidator.isSupportUser(corpUserInfo(false, true)), true);
  }

  @Test
  public void testEscalatedEffectiveActorUsesSessionAuditStampForSystemCorpUser() {
    corpUserInfoByUrn.put(ADMIN_ACTOR_URN, corpUserInfo(true, false));

    CorpUserInfo proposed = corpUserInfo(false, true);
    AuditStamp sessionAudit =
        new AuditStamp().setActor(ADMIN_ACTOR_URN).setTime(System.currentTimeMillis());

    long violations =
        validate(
                proposed,
                null,
                operationContext(INTRINSIC_SYSTEM_ACTOR_URN, ADMIN_ACTOR_URN),
                null,
                sessionAudit)
            .count();
    assertEquals(violations, 0);
  }

  @Test
  public void testEscalatedEffectiveActorDoesNotBypassRegularSessionUser() {
    corpUserInfoByUrn.put(REGULAR_ACTOR_URN, corpUserInfo(false, false));

    CorpUserInfo proposed = corpUserInfo(false, true);
    AuditStamp sessionAudit =
        new AuditStamp().setActor(REGULAR_ACTOR_URN).setTime(System.currentTimeMillis());

    long violations =
        validate(
                proposed,
                null,
                operationContext(INTRINSIC_SYSTEM_ACTOR_URN, REGULAR_ACTOR_URN),
                null,
                sessionAudit)
            .count();
    assertEquals(violations, 1);
  }

  @Test
  public void testIntrinsicSystemActorAllowedWithoutCorpUserInfo() {
    CorpUserInfo proposed = corpUserInfo(false, true);
    AuditStamp sessionAudit =
        new AuditStamp().setActor(INTRINSIC_SYSTEM_ACTOR_URN).setTime(System.currentTimeMillis());

    long violations =
        validate(
                proposed,
                null,
                operationContext(INTRINSIC_SYSTEM_ACTOR_URN, INTRINSIC_SYSTEM_ACTOR_URN),
                null,
                sessionAudit)
            .count();
    assertEquals(violations, 0);
  }

  private java.util.stream.Stream<AspectValidationException> validate(
      CorpUserInfo proposed,
      CorpUserInfo current,
      OperationFingerprint operationContext,
      SystemMetadata systemMetadata) {
    return validate(proposed, current, operationContext, systemMetadata, null);
  }

  private java.util.stream.Stream<AspectValidationException> validate(
      CorpUserInfo proposed,
      CorpUserInfo current,
      OperationFingerprint operationContext,
      SystemMetadata systemMetadata,
      AuditStamp auditStamp) {
    if (current != null) {
      corpUserInfoByUrn.put(TARGET_USER_URN, current);
    }

    return validator.validateProposed(
        operationContext,
        Set.of(
            TestMCP.builder()
                .changeType(ChangeType.UPSERT)
                .urn(TARGET_USER_URN)
                .entitySpec(entityRegistry.getEntitySpec(TARGET_USER_URN.getEntityType()))
                .aspectSpec(
                    entityRegistry
                        .getEntitySpec(TARGET_USER_URN.getEntityType())
                        .getAspectSpec(CORP_USER_INFO_ASPECT_NAME))
                .recordTemplate(proposed)
                .systemMetadata(systemMetadata)
                .auditStamp(auditStamp)
                .build()),
        mockRetrieverContext,
        null);
  }

  private static CorpUserInfo corpUserInfo(boolean system, boolean support) {
    CorpUserInfo info = new CorpUserInfo();
    info.setSystem(system);
    if (support) {
      info.data().put("isSupportUser", true);
    }
    return info;
  }

  private static SystemMetadata systemMetadataWithSource(String appSource) {
    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, appSource);
    systemMetadata.setProperties(properties);
    return systemMetadata;
  }

  private static OperationFingerprint operationContext(Urn actorUrn) {
    return operationContext(actorUrn, actorUrn);
  }

  private static OperationFingerprint operationContext(Urn effectiveActorUrn, Urn auditActorUrn) {
    AuditStamp auditStamp =
        new AuditStamp().setActor(auditActorUrn).setTime(System.currentTimeMillis());
    return new OperationFingerprint() {
      @Override
      public Urn getActor() {
        return effectiveActorUrn;
      }

      @Override
      public String getRequestID() {
        return "test-request";
      }

      @Override
      public AuditStamp getAuditStamp() {
        return auditStamp;
      }

      @Override
      public String getGlobalContextId() {
        return "test-global";
      }

      @Override
      public String getSearchContextId() {
        return "test-search";
      }

      @Override
      public String getEntityContextId() {
        return "test-entity";
      }
    };
  }
}
