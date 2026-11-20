package com.linkedin.metadata.aspect.validation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.ApiOperation;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.stream.Stream;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DiscoveredDataSourceAuthorizationValidatorTest {

  private static final Urn SOURCE_URN = UrnUtils.getUrn("urn:li:discoveredDataSource:mysql");

  private DiscoveredDataSourceAuthorizationValidator validator;
  private AuthorizationSession session;
  private MockedStatic<AuthUtil> authUtil;

  @BeforeMethod
  public void setup() {
    authUtil = Mockito.mockStatic(AuthUtil.class);
    validator =
        new DiscoveredDataSourceAuthorizationValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className(DiscoveredDataSourceAuthorizationValidator.class.getName())
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT", "DELETE"))
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName("discoveredDataSource")
                                .aspectName("*")
                                .build()))
                    .build());
    session = Mockito.mock(AuthorizationSession.class);
  }

  @AfterMethod
  public void tearDown() {
    authUtil.close();
  }

  @Test
  public void testDeniedWithoutPrivilege() {
    authUtil
        .when(() -> AuthUtil.isAuthorizedEntityUrns(eq(session), any(), any()))
        .thenReturn(false);

    Assert.assertTrue(validate(ChangeType.UPSERT).findAny().isPresent());
  }

  @Test
  public void testAllowedWithPrivilegeForTheOperation() {
    authUtil
        .when(
            () ->
                AuthUtil.isAuthorizedEntityUrns(
                    eq(session), eq(ApiOperation.DELETE), eq(List.of(SOURCE_URN))))
        .thenReturn(true);

    Assert.assertTrue(validate(ChangeType.DELETE).findAny().isEmpty());
    // Delete rights alone do not allow edits.
    Assert.assertTrue(validate(ChangeType.UPSERT).findAny().isPresent());
  }

  private Stream<AspectValidationException> validate(ChangeType changeType) {
    BatchItem item = Mockito.mock(BatchItem.class);
    Mockito.when(item.getUrn()).thenReturn(SOURCE_URN);
    Mockito.when(item.getChangeType()).thenReturn(changeType);
    return validator.validateProposedAspectsWithAuth(
        OperationFingerprint.EMPTY,
        List.of(item),
        TestOperationContexts.systemContextNoSearchAuthorization().getRetrieverContext(),
        session);
  }
}
