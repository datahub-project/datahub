package com.linkedin.metadata.models;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static org.testng.Assert.*;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.validation.SystemDataWriteValidator;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.SystemDataVisibility;
import com.linkedin.metadata.models.annotation.SystemEntityAnnotation;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SystemDataWriteValidatorTest {

  private static final AspectPluginConfig CONFIG =
      AspectPluginConfig.builder()
          .className(SystemDataWriteValidator.class.getName())
          .enabled(true)
          .supportedOperations(
              List.of("CREATE", "CREATE_ENTITY", "UPDATE", "UPSERT", "PATCH", "DELETE", "RESTATE"))
          .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
          .build();

  private SystemDataWriteValidator validator;
  private ConfigEntitySpec systemEntitySpec;
  private ConfigEntitySpec normalEntitySpec;

  @BeforeMethod
  public void setup() {
    validator = new SystemDataWriteValidator().setConfig(CONFIG);

    AspectSpec systemKey = aspect("systemKey", true);
    AspectSpec systemValue = aspect("systemValue", false);
    systemEntitySpec =
        new ConfigEntitySpec("testSystem", "systemKey", List.of(systemKey, systemValue), "default");
    systemEntitySpec.setSystemEntityFlags(true, false, false);

    AspectSpec normalKey = aspect("normalKey", false);
    AspectSpec normalValue = aspect("normalValue", false);
    normalEntitySpec =
        new ConfigEntitySpec("normal", "normalKey", List.of(normalKey, normalValue), "default");
  }

  @Test
  public void testSystemEntityWriteAllowedForUserWhenValidatorDisabled() {
    SystemDataWriteValidator disabledValidator =
        new SystemDataWriteValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className(SystemDataWriteValidator.class.getName())
                    .enabled(false)
                    .supportedOperations(CONFIG.getSupportedOperations())
                    .supportedEntityAspectNames(CONFIG.getSupportedEntityAspectNames())
                    .build());
    assertEquals(
        disabledValidator
            .validateProposed(
                fingerprintForActor("urn:li:corpuser:test"),
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.CREATE_ENTITY)
                        .urn(UrnUtils.getUrn("urn:li:testSystem:abc"))
                        .entitySpec(systemEntitySpec)
                        .aspectSpec(systemEntitySpec.getKeyAspectSpec())
                        .auditStamp(userAudit())
                        .build()),
                null,
                null)
            .count(),
        0);
  }

  @Test
  public void testSystemEntityWriteDeniedForUser() {
    assertEquals(
        validator
            .validateProposed(
                fingerprintForActor("urn:li:corpuser:test"),
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.CREATE_ENTITY)
                        .urn(UrnUtils.getUrn("urn:li:testSystem:abc"))
                        .entitySpec(systemEntitySpec)
                        .aspectSpec(systemEntitySpec.getKeyAspectSpec())
                        .auditStamp(userAudit())
                        .build()),
                null,
                null)
            .count(),
        1);
  }

  @Test
  public void testSystemEntityWriteAllowedForSystemActor() {
    assertEquals(
        validator
            .validateProposed(
                fingerprintForActor(SYSTEM_ACTOR),
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(UrnUtils.getUrn("urn:li:testSystem:abc"))
                        .entitySpec(systemEntitySpec)
                        .aspectSpec(systemEntitySpec.getAspectSpec("systemValue"))
                        .auditStamp(systemAudit())
                        .build()),
                null,
                null)
            .count(),
        0);
  }

  @Test
  public void testSystemEntityWriteDeniedWhenProposalAuditStampIsSpoofed() {
    assertEquals(
        validator
            .validateProposed(
                fingerprintForActor("urn:li:corpuser:test"),
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(UrnUtils.getUrn("urn:li:testSystem:abc"))
                        .entitySpec(systemEntitySpec)
                        .aspectSpec(systemEntitySpec.getAspectSpec("systemValue"))
                        .auditStamp(systemAudit())
                        .build()),
                null,
                null)
            .count(),
        1);
  }

  @Test
  public void testNormalEntityUnchanged() {
    assertEquals(
        validator
            .validateProposed(
                fingerprintForActor("urn:li:corpuser:test"),
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(UrnUtils.getUrn("urn:li:normal:abc"))
                        .entitySpec(normalEntitySpec)
                        .aspectSpec(normalEntitySpec.getAspectSpec("normalValue"))
                        .auditStamp(userAudit())
                        .build()),
                null,
                null)
            .count(),
        0);
  }

  private static AspectSpec aspect(String name, boolean systemEntityKey) {
    AspectSpec spec =
        new AspectSpec(
            new AspectAnnotation(name, false, false, null, 0),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            null);
    if (systemEntityKey) {
      spec.setSystemEntityAnnotation(
          new SystemEntityAnnotation(SystemDataVisibility.fullyHidden()));
    }
    return spec;
  }

  private static AuditStamp userAudit() {
    return new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:test")).setTime(1L);
  }

  private static AuditStamp systemAudit() {
    return new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(1L);
  }

  private static OperationFingerprint fingerprintForActor(@Nonnull final String actorUrn) {
    final Urn actor = UrnUtils.getUrn(actorUrn);
    final AuditStamp auditStamp = new AuditStamp().setActor(actor).setTime(1L);
    return new OperationFingerprint() {
      @Nonnull
      @Override
      public Urn getActor() {
        return actor;
      }

      @Nonnull
      @Override
      public String getRequestID() {
        return "test-request";
      }

      @Nonnull
      @Override
      public AuditStamp getAuditStamp() {
        return auditStamp;
      }

      @Nonnull
      @Override
      public String getGlobalContextId() {
        return "test-global";
      }

      @Nonnull
      @Override
      public String getSearchContextId() {
        return "test-search";
      }

      @Nonnull
      @Override
      public String getEntityContextId() {
        return "test-entity";
      }
    };
  }
}
