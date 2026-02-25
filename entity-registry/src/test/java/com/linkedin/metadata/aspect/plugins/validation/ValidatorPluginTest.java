package com.linkedin.metadata.aspect.plugins.validation;

import static org.testng.Assert.*;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.test.TestEntityProfile;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ValidatorPluginTest {
  public static String REGISTRY_FILE = "test-entity-registry-plugins-1.yml";

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testCustomValidator() {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class.getClassLoader().getResourceAsStream(REGISTRY_FILE));

    List<AspectPayloadValidator> validators =
        configEntityRegistry.getAllAspectPayloadValidators().stream()
            .filter(validator -> validator.shouldApply(ChangeType.UPSERT, "chart", "status"))
            .collect(Collectors.toList());

    assertEquals(
        validators,
        List.of(
            new TestValidator()
                .setConfig(
                    AspectPluginConfig.builder()
                        .className(
                            "com.linkedin.metadata.aspect.plugins.validation.ValidatorPluginTest$TestValidator")
                        .supportedOperations(List.of("UPSERT"))
                        .enabled(true)
                        .supportedEntityAspectNames(
                            List.of(
                                AspectPluginConfig.EntityAspectName.builder()
                                    .entityName("*")
                                    .aspectName("status")
                                    .build()))
                        .build()),
            new TestValidator()
                .setConfig(
                    AspectPluginConfig.builder()
                        .className(
                            "com.linkedin.metadata.aspect.plugins.validation.ValidatorPluginTest$TestValidator")
                        .supportedOperations(List.of("UPSERT"))
                        .enabled(true)
                        .supportedEntityAspectNames(
                            List.of(
                                AspectPluginConfig.EntityAspectName.builder()
                                    .entityName("chart")
                                    .aspectName("status")
                                    .build()))
                        .build())));
  }

  @Test
  public void testValidatorAuthVsNonAuth() throws Exception {
    TestValidator validator = new TestValidator();

    BatchItem item = Mockito.mock(BatchItem.class);
    Mockito.when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
    Mockito.when(item.getUrn()).thenReturn(Urn.createFromString("urn:li:chart:test"));
    Mockito.when(item.getAspectName()).thenReturn("status");

    RetrieverContext retrieverContext = Mockito.mock(RetrieverContext.class);

    // With session
    AuthorizationSession session = Mockito.mock(AuthorizationSession.class);

    validator.validateProposed(List.of(item), retrieverContext, session);
    assertTrue(validator.isAuthMethodCalled());
    assertTrue(validator.isNoAuthMethodCalled());

    // Reset flags
    validator.setAuthMethodCalled(false);
    validator.setNoAuthMethodCalled(false);

    // Without session
    validator.validateProposed(List.of(item), retrieverContext, null);
    assertFalse(validator.isAuthMethodCalled());
    assertTrue(validator.isNoAuthMethodCalled());
  }

  @Getter
  @Setter
  @Accessors(chain = true)
  public static class TestValidator extends AspectPayloadValidator {

    private AspectPluginConfig config;
    private boolean authMethodCalled = false;
    private boolean noAuthMethodCalled = false;
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    @Override
    protected Stream<AspectValidationException> validateProposedAspects(
        @Nonnull Collection<? extends BatchItem> mcpItems,
        @Nonnull RetrieverContext retrieverContext) {
      noAuthMethodCalled = true;
      return Stream.empty();
    }

    @Override
    protected Stream<AspectValidationException> validateProposedAspectsWithAuth(
        @Nonnull Collection<? extends BatchItem> mcpItems,
        @Nonnull RetrieverContext retrieverContext,
        @Nullable com.datahub.authorization.AuthorizationSession session) {
      if (session != null) {
        authMethodCalled = true;
      }
      return Stream.empty();
    }

    @Override
    protected Stream<AspectValidationException> validatePreCommitAspects(
        @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
      return Stream.empty();
    }

    @Override
    public boolean shouldApply(ChangeType changeType, Urn urn, String aspectName) {
      return true;
    }
  }
}
