package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.identity.CorpUserViewsSettings;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.GlobalViewsSettings;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SettingsServiceTest {

  private static final Urn TEST_VIEW_URN = UrnUtils.getUrn("urn:li:dataHubView:test");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static OperationContext opContext =
      TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);

  @Test
  private static void testGetCorpUserSettingsNullSettings() throws Exception {
    final SettingsService service = new SettingsService(getCorpUserSettingsEntityClientMock(null));
    final CorpUserSettings res = service.getCorpUserSettings(opContext, TEST_USER_URN);
    Assert.assertNull(res);
  }

  @Test
  private static void testGetCorpUserSettingsValidSettings() throws Exception {
    final CorpUserSettings existingSettings =
        new CorpUserSettings()
            .setViews(new CorpUserViewsSettings().setDefaultView(TEST_VIEW_URN))
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true));

    final SettingsService service =
        new SettingsService(getCorpUserSettingsEntityClientMock(existingSettings));

    final CorpUserSettings res = service.getCorpUserSettings(opContext, TEST_USER_URN);
    Assert.assertEquals(existingSettings, res);
  }

  @Test
  private static void testGetCorpUserSettingsSettingsException() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.CORP_USER_ENTITY_NAME),
                Mockito.eq(TEST_USER_URN),
                Mockito.eq(ImmutableSet.of(Constants.CORP_USER_SETTINGS_ASPECT_NAME))))
        .thenThrow(new RemoteInvocationException());

    final SettingsService service = new SettingsService(mockClient);

    Assert.assertThrows(
        RuntimeException.class, () -> service.getCorpUserSettings(opContext, TEST_USER_URN));
  }

  @Test
  private static void testUpdateCorpUserSettingsValidSettings() throws Exception {

    final CorpUserSettings newSettings =
        new CorpUserSettings()
            .setViews(new CorpUserViewsSettings().setDefaultView(TEST_VIEW_URN))
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true));

    final MetadataChangeProposal expectedProposal =
        buildUpdateCorpUserSettingsChangeProposal(TEST_USER_URN, newSettings);

    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class), Mockito.eq(expectedProposal), Mockito.eq(false)))
        .thenReturn(TEST_USER_URN.toString());

    final SettingsService service = new SettingsService(mockClient);

    service.updateCorpUserSettings(opContext, TEST_USER_URN, newSettings);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), Mockito.eq(expectedProposal), Mockito.eq(false));
  }

  @Test
  private static void testUpdateCorpUserSettingsSettingsException() throws Exception {

    final CorpUserSettings newSettings =
        new CorpUserSettings()
            .setViews(new CorpUserViewsSettings().setDefaultView(TEST_VIEW_URN))
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true));

    final MetadataChangeProposal expectedProposal =
        buildUpdateCorpUserSettingsChangeProposal(TEST_USER_URN, newSettings);

    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class), Mockito.eq(expectedProposal), Mockito.eq(false)))
        .thenThrow(new RemoteInvocationException());

    final SettingsService service = new SettingsService(mockClient);

    Assert.assertThrows(
        RuntimeException.class,
        () -> service.updateCorpUserSettings(opContext, TEST_USER_URN, newSettings));
  }

  @Test
  private static void testGetGlobalSettingsNullSettings() throws Exception {
    final SettingsService service = new SettingsService(getGlobalSettingsEntityClientMock(null));
    final GlobalSettingsInfo res = service.getGlobalSettings(opContext);
    Assert.assertNull(res);
  }

  @Test
  private static void testGetGlobalSettingsValidSettings() throws Exception {
    final GlobalSettingsInfo existingSettings =
        new GlobalSettingsInfo().setViews(new GlobalViewsSettings().setDefaultView(TEST_VIEW_URN));

    final SettingsService service =
        new SettingsService(getGlobalSettingsEntityClientMock(existingSettings));

    final GlobalSettingsInfo res = service.getGlobalSettings(opContext);
    Assert.assertEquals(existingSettings, res);
  }

  @Test
  private static void testGetGlobalSettingsSettingsException() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(GLOBAL_SETTINGS_ENTITY_NAME),
                Mockito.eq(GLOBAL_SETTINGS_URN),
                Mockito.eq(ImmutableSet.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME))))
        .thenThrow(new RemoteInvocationException());

    final SettingsService service = new SettingsService(mockClient);

    Assert.assertThrows(RuntimeException.class, () -> service.getGlobalSettings(opContext));
  }

  @Test
  private static void testUpdateGlobalSettingsValidSettings() throws Exception {

    final GlobalSettingsInfo newSettings =
        new GlobalSettingsInfo().setViews(new GlobalViewsSettings().setDefaultView(TEST_VIEW_URN));

    final MetadataChangeProposal expectedProposal =
        buildUpdateGlobalSettingsChangeProposal(newSettings);

    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class), Mockito.eq(expectedProposal), Mockito.eq(false)))
        .thenReturn(GLOBAL_SETTINGS_URN.toString());

    final SettingsService service = new SettingsService(mockClient);

    service.updateGlobalSettings(opContext, newSettings);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), Mockito.eq(expectedProposal), Mockito.eq(false));
  }

  @Test
  private static void testUpdateGlobalSettingsSettingsException() throws Exception {

    final GlobalSettingsInfo newSettings =
        new GlobalSettingsInfo().setViews(new GlobalViewsSettings().setDefaultView(TEST_VIEW_URN));

    final MetadataChangeProposal expectedProposal =
        buildUpdateGlobalSettingsChangeProposal(newSettings);

    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class), Mockito.eq(expectedProposal), Mockito.eq(false)))
        .thenThrow(new RemoteInvocationException());

    final SettingsService service = new SettingsService(mockClient);

    Assert.assertThrows(
        RuntimeException.class, () -> service.updateGlobalSettings(opContext, newSettings));
  }

  private static SystemEntityClient getCorpUserSettingsEntityClientMock(
      @Nullable final CorpUserSettings settings) throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);

    EnvelopedAspectMap aspectMap =
        settings != null
            ? new EnvelopedAspectMap(
                ImmutableMap.of(
                    Constants.CORP_USER_SETTINGS_ASPECT_NAME,
                    new EnvelopedAspect().setValue(new Aspect(settings.data()))))
            : new EnvelopedAspectMap();

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.CORP_USER_ENTITY_NAME),
                Mockito.eq(TEST_USER_URN),
                Mockito.eq(ImmutableSet.of(Constants.CORP_USER_SETTINGS_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setEntityName(Constants.CORP_USER_ENTITY_NAME)
                .setUrn(TEST_USER_URN)
                .setAspects(aspectMap));
    return mockClient;
  }

  private static SystemEntityClient getGlobalSettingsEntityClientMock(
      @Nullable final GlobalSettingsInfo settings) throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);

    EnvelopedAspectMap aspectMap =
        settings != null
            ? new EnvelopedAspectMap(
                ImmutableMap.of(
                    GLOBAL_SETTINGS_INFO_ASPECT_NAME,
                    new EnvelopedAspect().setValue(new Aspect(settings.data()))))
            : new EnvelopedAspectMap();

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(GLOBAL_SETTINGS_ENTITY_NAME),
                Mockito.eq(GLOBAL_SETTINGS_URN),
                Mockito.eq(ImmutableSet.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setEntityName(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME)
                .setUrn(GLOBAL_SETTINGS_URN)
                .setAspects(aspectMap));
    return mockClient;
  }

  private static MetadataChangeProposal buildUpdateCorpUserSettingsChangeProposal(
      final Urn urn, final CorpUserSettings newSettings) {
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(CORP_USER_ENTITY_NAME);
    mcp.setAspectName(CORP_USER_SETTINGS_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(newSettings));
    return mcp;
  }

  private static MetadataChangeProposal buildUpdateGlobalSettingsChangeProposal(
      final GlobalSettingsInfo newSettings) {
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(GLOBAL_SETTINGS_URN);
    mcp.setEntityType(GLOBAL_SETTINGS_ENTITY_NAME);
    mcp.setAspectName(GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(newSettings));
    return mcp;
  }
}
