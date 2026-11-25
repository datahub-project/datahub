package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.CORP_USER_CREDENTIALS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_INFO_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.PoliciesConfig;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.stream.Stream;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CorpUserCredentialsAuthorizationValidatorTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:existing-user");

  private CorpUserCredentialsAuthorizationValidator validator;
  private AuthorizationSession mockAuthSession;
  private MockedStatic<AuthUtil> authUtil;

  @BeforeMethod
  public void setup() {
    authUtil = Mockito.mockStatic(AuthUtil.class);
    validator =
        new CorpUserCredentialsAuthorizationValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className(CorpUserCredentialsAuthorizationValidator.class.getName())
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT", "PATCH", "DELETE"))
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName("corpuser")
                                .aspectName(CORP_USER_CREDENTIALS_ASPECT_NAME)
                                .build()))
                    .build());
    mockAuthSession = mock(AuthorizationSession.class);
  }

  @AfterMethod
  public void tearDown() {
    authUtil.close();
  }

  @Test
  public void testUpsertDeniedWithoutPrivilege() {
    denyCredentialsPrivilege();

    Assert.assertTrue(
        validate(OperationFingerprint.EMPTY, ChangeType.UPSERT).findAny().isPresent());
  }

  @Test
  public void testPatchDeniedWithoutPrivilege() {
    denyCredentialsPrivilege();

    Assert.assertTrue(validate(OperationFingerprint.EMPTY, ChangeType.PATCH).findAny().isPresent());
  }

  @Test
  public void testUpsertAllowedWithPrivilege() {
    authUtil
        .when(
            () ->
                AuthUtil.isAuthorized(
                    mockAuthSession, PoliciesConfig.MANAGE_USER_CREDENTIALS_PRIVILEGE))
        .thenReturn(true);

    Assert.assertTrue(validate(OperationFingerprint.EMPTY, ChangeType.UPSERT).findAny().isEmpty());
  }

  @Test
  public void testSystemSessionSkipsAuthorization() {
    denyCredentialsPrivilege();
    OperationFingerprint system = mock(OperationFingerprint.class);
    when(system.isSystemAuth()).thenReturn(true);

    Assert.assertTrue(validate(system, ChangeType.UPSERT).findAny().isEmpty());
  }

  @Test
  public void testOtherAspectsAreNotChecked() {
    denyCredentialsPrivilege();

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY,
            List.of(item(CORP_USER_INFO_ASPECT_NAME, ChangeType.UPSERT)),
            TestOperationContexts.systemContextNoSearchAuthorization().getRetrieverContext(),
            mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  private void denyCredentialsPrivilege() {
    authUtil
        .when(
            () ->
                AuthUtil.isAuthorized(
                    mockAuthSession, PoliciesConfig.MANAGE_USER_CREDENTIALS_PRIVILEGE))
        .thenReturn(false);
  }

  private Stream<AspectValidationException> validate(
      OperationFingerprint operation, ChangeType changeType) {
    return validator.validateProposedAspectsWithAuth(
        operation,
        List.of(item(CORP_USER_CREDENTIALS_ASPECT_NAME, changeType)),
        TestOperationContexts.systemContextNoSearchAuthorization().getRetrieverContext(),
        mockAuthSession);
  }

  private static BatchItem item(String aspectName, ChangeType changeType) {
    BatchItem item = mock(BatchItem.class);
    when(item.getUrn()).thenReturn(TEST_USER_URN);
    when(item.getAspectName()).thenReturn(aspectName);
    when(item.getChangeType()).thenReturn(changeType);
    return item;
  }
}
