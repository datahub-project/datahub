package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DATAHUB_POLICY_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.POLICY_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PolicyWriteAuthorizationValidatorTest {

  private static final Urn POLICY_URN = UrnUtils.getUrn("urn:li:dataHubPolicy:test-policy");

  private PolicyWriteAuthorizationValidator validator;
  private AuthorizationSession mockAuthSession;
  private MockedStatic<AuthUtil> authUtilMockedStatic;

  @BeforeMethod
  public void setup() {
    authUtilMockedStatic = Mockito.mockStatic(AuthUtil.class);
    validator = new PolicyWriteAuthorizationValidator();
    validator.setConfig(
        AspectPluginConfig.builder()
            .className(PolicyWriteAuthorizationValidator.class.getName())
            .enabled(true)
            .supportedOperations(List.of("UPSERT", "PATCH"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName(POLICY_ENTITY_NAME)
                        .aspectName(DATAHUB_POLICY_INFO_ASPECT_NAME)
                        .build()))
            .build());
    mockAuthSession = Mockito.mock(AuthorizationSession.class);
  }

  @AfterMethod
  public void tearDown() {
    authUtilMockedStatic.close();
  }

  @Test
  public void testDenyWithoutManagePolicies() {
    stubManagePolicies(false);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY,
            Collections.singletonList(buildItem()),
            TestOperationContexts.systemContextNoSearchAuthorization().getRetrieverContext(),
            mockAuthSession);

    Assert.assertTrue(result.findAny().isPresent());
  }

  @Test
  public void testAllowWithManagePolicies() {
    stubManagePolicies(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY,
            Collections.singletonList(buildItem()),
            TestOperationContexts.systemContextNoSearchAuthorization().getRetrieverContext(),
            mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  private void stubManagePolicies(boolean allowed) {
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorizedEntityType(
                    eq(mockAuthSession), eq(ApiOperation.MANAGE), any()))
        .thenReturn(allowed);
  }

  private static TestMCP buildItem() {
    DataHubPolicyInfo info = new DataHubPolicyInfo();
    info.setDisplayName("test");
    info.setDescription("test");
    info.setType("METADATA");
    info.setState("ACTIVE");
    info.setPrivileges(new StringArray("MANAGE_POLICIES"));
    info.setActors(new DataHubActorFilter());
    return TestMCP.ofOneUpsertItem(POLICY_URN, info, new TestEntityRegistry()).stream()
        .map(i -> (TestMCP) i)
        .findFirst()
        .get();
  }
}
