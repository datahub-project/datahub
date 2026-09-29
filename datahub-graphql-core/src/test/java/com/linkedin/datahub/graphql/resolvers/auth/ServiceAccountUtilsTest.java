package com.linkedin.datahub.graphql.resolvers.auth;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ServiceAccount;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.identity.CorpUserViewsSettings;
import com.linkedin.metadata.Constants;
import org.testng.annotations.Test;

public class ServiceAccountUtilsTest {

  private static final String TEST_SERVICE_ACCOUNT_URN =
      "urn:li:corpuser:service_test-service-account";
  private static final String TEST_SERVICE_ACCOUNT_NAME = "test-service-account";

  @Test
  public void testIsServiceAccountWithValidServiceAccount() throws Exception {
    EntityResponse response = createMockEntityResponse(true, true);
    assertTrue(ServiceAccountUtils.isServiceAccount(response));
  }

  @Test
  public void testIsServiceAccountWithRegularUser() throws Exception {
    EntityResponse response = createMockEntityResponse(false, false);
    assertFalse(ServiceAccountUtils.isServiceAccount(response));
  }

  @Test
  public void testIsServiceAccountWithNullResponse() {
    assertFalse(ServiceAccountUtils.isServiceAccount(null));
  }

  @Test
  public void testIsServiceAccountWithNoSubTypesAspect() throws Exception {
    EntityResponse response = mock(EntityResponse.class);
    when(response.getUrn()).thenReturn(Urn.createFromString(TEST_SERVICE_ACCOUNT_URN));
    when(response.getAspects()).thenReturn(new EnvelopedAspectMap());
    assertFalse(ServiceAccountUtils.isServiceAccount(response));
  }

  @Test
  public void testExtractNameFromUrnWithPrefix() {
    String result = ServiceAccountUtils.extractNameFromUrn(TEST_SERVICE_ACCOUNT_URN);
    assertEquals(result, TEST_SERVICE_ACCOUNT_NAME);
  }

  @Test
  public void testExtractNameFromUrnWithoutPrefix() {
    String result = ServiceAccountUtils.extractNameFromUrn("urn:li:corpuser:regular-user");
    assertEquals(result, "regular-user");
  }

  @Test
  public void testBuildServiceAccountUrn() {
    String result = ServiceAccountUtils.buildServiceAccountUrn(TEST_SERVICE_ACCOUNT_NAME);
    assertEquals(result, TEST_SERVICE_ACCOUNT_URN);
  }

  @Test
  public void testBuildServiceAccountId() {
    String result = ServiceAccountUtils.buildServiceAccountId(TEST_SERVICE_ACCOUNT_NAME);
    assertEquals(result, "service_" + TEST_SERVICE_ACCOUNT_NAME);
  }

  @Test
  public void testMapToServiceAccountWithFullInfo() throws Exception {
    EntityResponse response = createMockEntityResponse(true, true);

    ServiceAccount result = ServiceAccountUtils.mapToServiceAccount(response);

    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_SERVICE_ACCOUNT_URN);
    assertEquals(result.getType(), EntityType.CORP_USER);
    assertEquals(result.getName(), TEST_SERVICE_ACCOUNT_NAME);
    assertEquals(result.getDisplayName(), "Test Service Account");
    assertEquals(result.getDescription(), "Test description");
  }

  @Test
  public void testMapToServiceAccountWithNullResponse() {
    ServiceAccount result = ServiceAccountUtils.mapToServiceAccount(null);
    assertNull(result);
  }

  @Test
  public void testMapToServiceAccountWithNoCorpUserInfo() throws Exception {
    EntityResponse response = createMockEntityResponse(true, false);

    ServiceAccount result = ServiceAccountUtils.mapToServiceAccount(response);

    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_SERVICE_ACCOUNT_URN);
    assertEquals(result.getName(), TEST_SERVICE_ACCOUNT_NAME);
    assertNull(result.getDisplayName());
    assertNull(result.getDescription());
  }

  @Test
  public void testMapToServiceAccountWithDefaultView() throws Exception {
    String viewUrn = "urn:li:dataHubView:my-view";
    EntityResponse response = createMockEntityResponse(true, true);

    // Add corpUserSettings with a default view
    CorpUserViewsSettings viewsSettings = new CorpUserViewsSettings();
    viewsSettings.setDefaultView(UrnUtils.getUrn(viewUrn));
    CorpUserSettings settings = new CorpUserSettings();
    settings.setViews(viewsSettings);
    EnvelopedAspect settingsAspect = new EnvelopedAspect();
    settingsAspect.setValue(new Aspect(settings.data()));
    response.getAspects().put(Constants.CORP_USER_SETTINGS_ASPECT_NAME, settingsAspect);

    ServiceAccount result = ServiceAccountUtils.mapToServiceAccount(response);

    assertNotNull(result);
    assertNotNull(result.getDefaultView());
    assertEquals(result.getDefaultView().getUrn(), viewUrn);
    assertEquals(result.getDefaultView().getType(), EntityType.DATAHUB_VIEW);
  }

  @Test
  public void testMapToServiceAccountWithNoDefaultView() throws Exception {
    EntityResponse response = createMockEntityResponse(true, true);

    // Add corpUserSettings without a default view
    CorpUserSettings settings = new CorpUserSettings();
    settings.setViews(new CorpUserViewsSettings());
    EnvelopedAspect settingsAspect = new EnvelopedAspect();
    settingsAspect.setValue(new Aspect(settings.data()));
    response.getAspects().put(Constants.CORP_USER_SETTINGS_ASPECT_NAME, settingsAspect);

    ServiceAccount result = ServiceAccountUtils.mapToServiceAccount(response);

    assertNotNull(result);
    assertNull(result.getDefaultView());
  }

  @Test
  public void testMapToServiceAccountWithNoSettingsAspect() throws Exception {
    // No corpUserSettings aspect at all — defaultView should be null
    EntityResponse response = createMockEntityResponse(true, false);

    ServiceAccount result = ServiceAccountUtils.mapToServiceAccount(response);

    assertNotNull(result);
    assertNull(result.getDefaultView());
  }

  @Test
  public void testGetOrCreateSettingsWithNull() {
    CorpUserSettings result = ServiceAccountUtils.getOrCreateSettings(null);
    assertNotNull(result);
    assertTrue(result.hasAppearance());
    assertFalse(result.getAppearance().isShowSimplifiedHomepage());
  }

  @Test
  public void testGetOrCreateSettingsWithExisting() {
    CorpUserSettings existing =
        new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true))
            .setViews(
                new CorpUserViewsSettings()
                    .setDefaultView(UrnUtils.getUrn("urn:li:dataHubView:existing")));

    CorpUserSettings result = ServiceAccountUtils.getOrCreateSettings(existing);
    assertSame(result, existing);
  }

  private EntityResponse createMockEntityResponse(
      boolean isServiceAccount, boolean includeCorpUserInfo) throws Exception {
    EntityResponse response = mock(EntityResponse.class);
    when(response.getUrn()).thenReturn(Urn.createFromString(TEST_SERVICE_ACCOUNT_URN));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    if (isServiceAccount) {
      SubTypes subTypes = new SubTypes();
      subTypes.setTypeNames(new StringArray(ServiceAccountUtils.SERVICE_ACCOUNT_SUB_TYPE));
      EnvelopedAspect subTypesAspect = new EnvelopedAspect();
      subTypesAspect.setValue(new Aspect(subTypes.data()));
      aspectMap.put(Constants.SUB_TYPES_ASPECT_NAME, subTypesAspect);
    }

    if (includeCorpUserInfo) {
      CorpUserInfo info = new CorpUserInfo();
      info.setActive(true);
      info.setDisplayName("Test Service Account");
      info.setTitle("Test description");
      EnvelopedAspect infoAspect = new EnvelopedAspect();
      infoAspect.setValue(new Aspect(info.data()));
      aspectMap.put(Constants.CORP_USER_INFO_ASPECT_NAME, infoAspect);
    }

    when(response.getAspects()).thenReturn(aspectMap);
    return response;
  }
}
