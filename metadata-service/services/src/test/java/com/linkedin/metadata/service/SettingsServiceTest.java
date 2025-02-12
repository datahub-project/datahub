package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.AspectUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpGroupSettings;
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
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SettingsServiceTest {
  private static final String USER_URN_STRING = "urn:li:corpuser:testUser";
  private static final Urn TEST_USER_URN = UrnUtils.getUrn(USER_URN_STRING);
  private static final Urn TEST_USER_URN_2 = UrnUtils.getUrn(USER_URN_STRING + "_2");
  private static final String GROUP_URN_STRING = "urn:li:corpGroup:testGroup";
  private static final Urn GROUP_URN = UrnUtils.getUrn(GROUP_URN_STRING);
  private static final Urn GROUP_URN_2 = UrnUtils.getUrn(GROUP_URN_STRING + "_2");
  private static final Urn TEST_VIEW_URN = UrnUtils.getUrn("urn:li:dataHubView:test");
  private static final String USER_HANDLE = "testUser";
  private static final List<String> CHANNELS = Collections.singletonList("testChannel");
  private static OperationContext opContext =
      TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);

  private static final SlackNotificationSettings USER_SLACK_NOTIFICATION_SETTINGS =
      new SlackNotificationSettings().setUserHandle(USER_HANDLE);
  private static final NotificationSettings USER_NOTIFICATION_SETTINGS =
      new NotificationSettings().setSlackSettings(USER_SLACK_NOTIFICATION_SETTINGS);
  private static final CorpUserSettings CORP_USER_SETTINGS =
      new CorpUserSettings()
          .setViews(new CorpUserViewsSettings().setDefaultView(TEST_VIEW_URN))
          .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true));

  private static final CorpUserSettings UPDATED_CORP_USER_SETTINGS =
      CORP_USER_SETTINGS.setNotificationSettings(USER_NOTIFICATION_SETTINGS);

  private static final EntityResponse CORP_USER_ENTITY_RESPONSE =
      createEntityResponseFromAspects(
          ImmutableMap.of(Constants.CORP_USER_SETTINGS_ASPECT_NAME, CORP_USER_SETTINGS));
  private static final SlackNotificationSettings GROUP_SLACK_NOTIFICATION_SETTINGS =
      new SlackNotificationSettings().setChannels(new StringArray(CHANNELS));
  private static final NotificationSettings GROUP_NOTIFICATION_SETTINGS =
      new NotificationSettings().setSlackSettings(GROUP_SLACK_NOTIFICATION_SETTINGS);
  private static final CorpGroupSettings CORP_GROUP_SETTINGS =
      new CorpGroupSettings().setNotificationSettings(GROUP_NOTIFICATION_SETTINGS);
  private static final CorpGroupSettings UPDATED_CORP_GROUP_SETTINGS = CORP_GROUP_SETTINGS;
  private static final EntityResponse CORP_GROUP_ENTITY_RESPONSE =
      createEntityResponseFromAspects(
          ImmutableMap.of(Constants.CORP_GROUP_SETTINGS_ASPECT_NAME, CORP_GROUP_SETTINGS));

  @Test
  private static void testGetCorpUserSettingsNullSettings() throws Exception {
    final SettingsService service =
        new SettingsService(
            getCorpUserSettingsEntityClientMock(null),
            mock(OpenApiClient.class),
            new ObjectMapper());
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
        new SettingsService(
            getCorpUserSettingsEntityClientMock(existingSettings),
            mock(OpenApiClient.class),
            new ObjectMapper());

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

    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());

    Assert.assertThrows(
        RuntimeException.class, () -> service.getCorpUserSettings(opContext, TEST_USER_URN));
  }

  @Test
  private static void testBatchGetCorpUserSettingsNullSettings() throws Exception {
    final SettingsService service =
        new SettingsService(
            getCorpUserSettingsEntityClientMock(null),
            mock(OpenApiClient.class),
            new ObjectMapper());
    final Map<Urn, CorpUserSettings> res =
        service.batchGetCorpUserSettings(
            opContext, ImmutableList.of(TEST_USER_URN, TEST_USER_URN_2));

    Assert.assertEquals(res.size(), 0);
  }

  @Test
  private static void testBatchGetCorpUserSettingsValidSettings() throws Exception {
    final CorpUserSettings existingSettings =
        new CorpUserSettings()
            .setViews(new CorpUserViewsSettings().setDefaultView(TEST_VIEW_URN))
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true));

    final SettingsService service =
        new SettingsService(
            getCorpUserSettingsEntityClientMock(existingSettings),
            mock(OpenApiClient.class),
            new ObjectMapper());

    final Map<Urn, CorpUserSettings> res =
        service.batchGetCorpUserSettings(
            opContext, ImmutableList.of(TEST_USER_URN, TEST_USER_URN_2));
    Assert.assertEquals(existingSettings, res.get(TEST_USER_URN));
    Assert.assertEquals(existingSettings, res.get(TEST_USER_URN_2));
  }

  @Test
  private static void testBatchGetCorpUserSettingsSettingsException() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.when(
            mockClient.batchGetV2(
                any(OperationContext.class),
                Mockito.eq(Constants.CORP_USER_ENTITY_NAME),
                Mockito.eq(ImmutableSet.of(TEST_USER_URN, TEST_USER_URN_2)),
                Mockito.eq(ImmutableSet.of(Constants.CORP_USER_SETTINGS_ASPECT_NAME))))
        .thenThrow(new RemoteInvocationException());

    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());

    Assert.assertThrows(
        RuntimeException.class,
        () ->
            service.batchGetCorpUserSettings(
                opContext, ImmutableList.of(TEST_USER_URN, TEST_USER_URN_2)));
  }

  @Test
  public void testGetCorpGroupSettingsMissingActor() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(GROUP_URN))).thenReturn(false);
    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());
    assertThrows(() -> service.getCorpUserSettings(mock(OperationContext.class), GROUP_URN));
  }

  @Test
  public void testGetCorpGroupSettingsNullSettings() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(GROUP_URN))).thenReturn(true);
    when(mockClient.getV2(
            any(OperationContext.class),
            eq(CORP_GROUP_ENTITY_NAME),
            eq(GROUP_URN),
            eq(ImmutableSet.of(CORP_GROUP_SETTINGS_ASPECT_NAME))))
        .thenReturn(null);
    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());
    final CorpGroupSettings res =
        service.getCorpGroupSettings(mock(OperationContext.class), GROUP_URN);
    Assert.assertNull(res);
  }

  @Test
  public void testGetCorpGroupSettingsValidSettings() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(GROUP_URN))).thenReturn(true);
    when(mockClient.getV2(
            any(OperationContext.class),
            eq(CORP_GROUP_ENTITY_NAME),
            eq(GROUP_URN),
            eq(ImmutableSet.of(CORP_GROUP_SETTINGS_ASPECT_NAME))))
        .thenReturn(CORP_GROUP_ENTITY_RESPONSE);
    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());
    final CorpGroupSettings res =
        service.getCorpGroupSettings(mock(OperationContext.class), GROUP_URN);
    assertEquals(CORP_GROUP_SETTINGS, res);
  }

  @Test
  public void testGetCorpGroupSettingsException() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(GROUP_URN))).thenReturn(true);
    when(mockClient.getV2(
            any(OperationContext.class),
            eq(CORP_GROUP_ENTITY_NAME),
            eq(GROUP_URN),
            eq(ImmutableSet.of(CORP_GROUP_SETTINGS_ASPECT_NAME))))
        .thenThrow(new RemoteInvocationException());
    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());
    assertThrows(
        RuntimeException.class,
        () -> service.getCorpUserSettings(mock(OperationContext.class), TEST_USER_URN));
  }

  @Test
  private static void testBatchGetCorpGroupSettingsNullSettings() throws Exception {
    final SettingsService service =
        new SettingsService(
            getCorpGroupSettingsEntityClientMock(null),
            mock(OpenApiClient.class),
            new ObjectMapper());
    final Map<Urn, CorpGroupSettings> res =
        service.batchGetCorpGroupSettings(opContext, ImmutableList.of(GROUP_URN, GROUP_URN_2));

    Assert.assertEquals(res.size(), 0);
  }

  @Test
  private static void testBatchGetCorpGroupSettingsValidSettings() throws Exception {
    final SettingsService service =
        new SettingsService(
            getCorpGroupSettingsEntityClientMock(CORP_GROUP_SETTINGS),
            mock(OpenApiClient.class),
            new ObjectMapper());

    final Map<Urn, CorpGroupSettings> res =
        service.batchGetCorpGroupSettings(opContext, ImmutableList.of(GROUP_URN, GROUP_URN_2));

    Assert.assertEquals(CORP_GROUP_SETTINGS, res.get(GROUP_URN));
    Assert.assertEquals(CORP_GROUP_SETTINGS, res.get(GROUP_URN_2));
  }

  @Test
  private static void testBatchGetCorpGroupSettingsSettingsException() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.when(
            mockClient.batchGetV2(
                any(OperationContext.class),
                Mockito.eq(CORP_GROUP_ENTITY_NAME),
                Mockito.eq(ImmutableSet.of(GROUP_URN, GROUP_URN_2)),
                Mockito.eq(ImmutableSet.of(CORP_GROUP_SETTINGS_ASPECT_NAME))))
        .thenThrow(new RemoteInvocationException());

    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());

    Assert.assertThrows(
        RuntimeException.class,
        () ->
            service.batchGetCorpGroupSettings(opContext, ImmutableList.of(GROUP_URN, GROUP_URN_2)));
  }

  @Test
  public void testUpdateCorpUserSettingsValidSettings() throws Exception {
    final MetadataChangeProposal expectedProposal =
        buildUpdateCorpUserSettingsChangeProposal(TEST_USER_URN, UPDATED_CORP_USER_SETTINGS);

    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_USER_URN))).thenReturn(true);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class), Mockito.eq(expectedProposal), Mockito.eq(false)))
        .thenReturn(TEST_USER_URN.toString());

    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());

    service.updateCorpUserSettings(opContext, TEST_USER_URN, UPDATED_CORP_USER_SETTINGS);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), Mockito.eq(expectedProposal), Mockito.eq(false));
  }

  @Test
  public void testUpdateCorpUserSettingsException() throws Exception {
    final MetadataChangeProposal expectedProposal =
        buildUpdateCorpUserSettingsChangeProposal(TEST_USER_URN, UPDATED_CORP_USER_SETTINGS);

    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class), Mockito.eq(expectedProposal), Mockito.eq(false)))
        .thenThrow(new RemoteInvocationException());

    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());

    Assert.assertThrows(
        RuntimeException.class,
        () -> service.updateCorpUserSettings(opContext, TEST_USER_URN, UPDATED_CORP_USER_SETTINGS));
  }

  @Test
  private static void testGetGlobalSettingsNullSettings() throws Exception {
    final SettingsService service =
        new SettingsService(
            getGlobalSettingsEntityClientMock(null), mock(OpenApiClient.class), new ObjectMapper());
    final GlobalSettingsInfo res = service.getGlobalSettings(opContext);
    Assert.assertNull(res);
  }

  @Test
  public void testGetGlobalSettingsValidSettings() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final GlobalSettingsInfo existingSettings =
        new GlobalSettingsInfo().setViews(new GlobalViewsSettings().setDefaultView(TEST_VIEW_URN));
    when(mockClient.getV2(
            any(OperationContext.class),
            eq(GLOBAL_SETTINGS_ENTITY_NAME),
            eq(GLOBAL_SETTINGS_URN),
            eq(ImmutableSet.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME))))
        .thenReturn(
            createEntityResponseFromAspects(
                ImmutableMap.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME, existingSettings)));

    final SettingsService service =
        new SettingsService(
            getGlobalSettingsEntityClientMock(existingSettings),
            mock(OpenApiClient.class),
            new ObjectMapper());

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

    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());

    Assert.assertThrows(RuntimeException.class, () -> service.getGlobalSettings(opContext));
  }

  @Test
  public void testUpdateGlobalSettingsValidSettings() throws Exception {
    final GlobalSettingsInfo newSettings =
        new GlobalSettingsInfo().setViews(new GlobalViewsSettings().setDefaultView(TEST_VIEW_URN));

    final MetadataChangeProposal expectedProposal =
        buildUpdateGlobalSettingsChangeProposal(newSettings);

    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class), Mockito.eq(expectedProposal), Mockito.eq(false)))
        .thenReturn(GLOBAL_SETTINGS_URN.toString());

    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());

    service.updateGlobalSettings(opContext, newSettings);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), Mockito.eq(expectedProposal), Mockito.eq(false));
  }

  @Test
  public void testUpdateGlobalSettingsSettingsException() throws Exception {
    final GlobalSettingsInfo newSettings =
        new GlobalSettingsInfo().setViews(new GlobalViewsSettings().setDefaultView(TEST_VIEW_URN));

    final MetadataChangeProposal expectedProposal =
        buildUpdateGlobalSettingsChangeProposal(newSettings);

    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class), Mockito.eq(expectedProposal), Mockito.eq(false)))
        .thenThrow(new RemoteInvocationException());

    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());

    Assert.assertThrows(
        RuntimeException.class, () -> service.updateGlobalSettings(opContext, newSettings));
  }

  private static SystemEntityClient getCorpUserSettingsEntityClientMock(
      @Nullable final CorpUserSettings settings) throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_USER_URN))).thenReturn(true);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_USER_URN), anyBoolean()))
        .thenReturn(true);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_USER_URN_2))).thenReturn(true);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_USER_URN_2), anyBoolean()))
        .thenReturn(true);

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

    Mockito.when(
            mockClient.batchGetV2(
                any(OperationContext.class),
                Mockito.eq(Constants.CORP_USER_ENTITY_NAME),
                Mockito.eq(ImmutableSet.of(TEST_USER_URN, TEST_USER_URN_2)),
                Mockito.eq(ImmutableSet.of(Constants.CORP_USER_SETTINGS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_USER_URN,
                new EntityResponse()
                    .setEntityName(Constants.CORP_USER_ENTITY_NAME)
                    .setUrn(TEST_USER_URN)
                    .setAspects(aspectMap),
                TEST_USER_URN_2,
                new EntityResponse()
                    .setEntityName(Constants.CORP_USER_ENTITY_NAME)
                    .setUrn(TEST_USER_URN_2)
                    .setAspects(aspectMap)));
    return mockClient;
  }

  private static SystemEntityClient getCorpGroupSettingsEntityClientMock(
      @Nullable final CorpGroupSettings settings) throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(GROUP_URN))).thenReturn(true);
    when(mockClient.exists(any(OperationContext.class), eq(GROUP_URN), anyBoolean()))
        .thenReturn(true);
    when(mockClient.exists(any(OperationContext.class), eq(GROUP_URN_2))).thenReturn(true);
    when(mockClient.exists(any(OperationContext.class), eq(GROUP_URN_2), anyBoolean()))
        .thenReturn(true);

    EnvelopedAspectMap aspectMap =
        settings != null
            ? new EnvelopedAspectMap(
                ImmutableMap.of(
                    CORP_GROUP_SETTINGS_ASPECT_NAME,
                    new EnvelopedAspect().setValue(new Aspect(settings.data()))))
            : new EnvelopedAspectMap();

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.CORP_GROUP_ENTITY_NAME),
                Mockito.eq(GROUP_URN),
                Mockito.eq(ImmutableSet.of(CORP_GROUP_SETTINGS_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setEntityName(Constants.CORP_GROUP_ENTITY_NAME)
                .setUrn(GROUP_URN)
                .setAspects(aspectMap));

    Mockito.when(
            mockClient.batchGetV2(
                any(OperationContext.class),
                Mockito.eq(Constants.CORP_GROUP_ENTITY_NAME),
                Mockito.eq(ImmutableSet.of(GROUP_URN, GROUP_URN_2)),
                Mockito.eq(ImmutableSet.of(Constants.CORP_GROUP_SETTINGS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                GROUP_URN,
                new EntityResponse()
                    .setEntityName(CORP_GROUP_ENTITY_NAME)
                    .setUrn(GROUP_URN)
                    .setAspects(aspectMap),
                GROUP_URN_2,
                new EntityResponse()
                    .setEntityName(CORP_GROUP_ENTITY_NAME)
                    .setUrn(GROUP_URN_2)
                    .setAspects(aspectMap)));
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

  private static MetadataChangeProposal buildUpdateCorpGroupSettingsChangeProposal(
      final Urn urn, final CorpGroupSettings newSettings) {
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(CORP_GROUP_ENTITY_NAME);
    mcp.setAspectName(CORP_GROUP_SETTINGS_ASPECT_NAME);
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

  // Saas Only
  @Test
  public void testGetCorpUserSettingsMissingActor() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_USER_URN))).thenReturn(false);
    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());
    assertThrows(() -> service.getCorpUserSettings(mock(OperationContext.class), TEST_USER_URN));
  }

  @Test
  public void testGetCorpUserSettingsException() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_USER_URN))).thenReturn(true);
    when(mockClient.getV2(
            any(OperationContext.class),
            eq(Constants.CORP_USER_ENTITY_NAME),
            eq(TEST_USER_URN),
            eq(ImmutableSet.of(Constants.CORP_USER_SETTINGS_ASPECT_NAME))))
        .thenThrow(new RemoteInvocationException());
    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());
    assertThrows(
        RuntimeException.class,
        () -> service.getCorpUserSettings(mock(OperationContext.class), TEST_USER_URN));
  }

  @Test
  public void testUpdateCorpGroupSettingsValidSettings() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final MetadataChangeProposal expectedProposal =
        buildUpdateCorpGroupSettingsChangeProposal(GROUP_URN, UPDATED_CORP_GROUP_SETTINGS);

    when(mockClient.exists(any(OperationContext.class), eq(GROUP_URN))).thenReturn(true);
    when(mockClient.ingestProposal(any(OperationContext.class), eq(expectedProposal), eq(true)))
        .thenReturn(GROUP_URN.toString());
    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());
    service.updateCorpGroupSettings(
        mock(OperationContext.class), GROUP_URN, UPDATED_CORP_GROUP_SETTINGS);

    verify(mockClient, times(1))
        .ingestProposal(any(OperationContext.class), eq(expectedProposal), eq(true));
  }

  @Test
  public void testUpdateCorpGroupSettingsException() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final MetadataChangeProposal expectedProposal =
        buildUpdateCorpGroupSettingsChangeProposal(GROUP_URN, UPDATED_CORP_GROUP_SETTINGS);

    when(mockClient.exists(any(OperationContext.class), eq(GROUP_URN))).thenReturn(true);
    when(mockClient.ingestProposal(any(OperationContext.class), eq(expectedProposal), eq(true)))
        .thenThrow(new RemoteInvocationException());
    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());
    assertThrows(
        () ->
            service.updateCorpGroupSettings(
                mock(OperationContext.class), GROUP_URN, UPDATED_CORP_GROUP_SETTINGS));
  }

  @Test
  public void testCreateSlackNotificationSettingsNullUserHandle() {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());
    final SlackNotificationSettings slackNotificationSettings =
        service.createSlackNotificationSettings(null, CHANNELS);
    assertFalse(slackNotificationSettings.hasUserHandle());
    assertTrue(slackNotificationSettings.hasChannels());
    assertEquals(slackNotificationSettings.getChannels(), CHANNELS);
  }

  @Test
  public void testCreateSlackNotificationSettingsNullChannels() {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final SettingsService service =
        new SettingsService(mockClient, mock(OpenApiClient.class), new ObjectMapper());
    final SlackNotificationSettings slackNotificationSettings =
        service.createSlackNotificationSettings(USER_HANDLE, null);
    assertTrue(slackNotificationSettings.hasUserHandle());
    assertEquals(slackNotificationSettings.getUserHandle(), USER_HANDLE);
    assertFalse(slackNotificationSettings.hasChannels());
  }
}
