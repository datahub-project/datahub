package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.ASSET_SETTINGS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.settings.asset.AssetSettings;
import com.linkedin.settings.asset.AssetSummarySettings;
import com.linkedin.settings.asset.AssetSummarySettingsTemplate;
import com.linkedin.settings.asset.AssetSummarySettingsTemplateArray;
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

public class AssetSettingsAuthorizationValidatorTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
  private static final Urn TEST_TEMPLATE_URN =
      UrnUtils.getUrn("urn:li:dataHubPageTemplate:test-template");

  private AssetSettingsAuthorizationValidator validator;
  private AuthorizationSession mockAuthSession;
  private MockedStatic<AuthUtil> authUtilMockedStatic;

  @BeforeMethod
  public void setup() {
    authUtilMockedStatic = Mockito.mockStatic(AuthUtil.class);
    validator = new AssetSettingsAuthorizationValidator();
    validator.setConfig(
        AspectPluginConfig.builder()
            .className(AssetSettingsAuthorizationValidator.class.getName())
            .enabled(true)
            .supportedOperations(List.of("UPSERT"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("*")
                        .aspectName(ASSET_SETTINGS_ASPECT_NAME)
                        .build()))
            .build());
    mockAuthSession = Mockito.mock(AuthorizationSession.class);
  }

  @AfterMethod
  public void tearDown() {
    authUtilMockedStatic.close();
  }

  @Test
  public void testDenyWithoutPrivilegeOnAsset() {
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    any(EntitySpec.class)))
        .thenReturn(false);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(buildItem()),
            TestOperationContexts.systemContextNoSearchAuthorization().getRetrieverContext(),
            mockAuthSession);

    Assert.assertTrue(result.findAny().isPresent());
  }

  @Test
  public void testAllowWithPrivilegeOnAsset() {
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession),
                    any(DisjunctivePrivilegeGroup.class),
                    eq(new EntitySpec("dataset", TEST_DATASET_URN.toString()))))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            Collections.singletonList(buildItem()),
            TestOperationContexts.systemContextNoSearchAuthorization().getRetrieverContext(),
            mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  private TestMCP buildItem() {
    AssetSummarySettingsTemplate template = new AssetSummarySettingsTemplate();
    template.setTemplate(TEST_TEMPLATE_URN);
    AssetSummarySettings summary = new AssetSummarySettings();
    summary.setTemplates(new AssetSummarySettingsTemplateArray(template));
    AssetSettings assetSettings = new AssetSettings();
    assetSettings.setAssetSummary(summary);

    return TestMCP.ofOneUpsertItem(TEST_DATASET_URN, assetSettings, new TestEntityRegistry())
        .stream()
        .map(i -> (TestMCP) i)
        .findFirst()
        .get();
  }
}
