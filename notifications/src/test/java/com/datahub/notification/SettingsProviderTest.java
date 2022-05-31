package com.datahub.notification;

import com.datahub.authentication.Authentication;
import com.datahub.notification.provider.SettingsProvider;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.settings.global.GlobalIntegrationSettings;
import com.linkedin.settings.global.GlobalNotificationSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.SlackIntegrationSettings;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;


public class SettingsProviderTest {

  @Test
  public void testGetGlobalSettings() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockClient.getV2(
        Mockito.eq(GLOBAL_SETTINGS_ENTITY_NAME),
        Mockito.eq(GLOBAL_SETTINGS_URN),
        Mockito.eq(ImmutableSet.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    )).thenReturn(
        mockSettingsResponse()
    );

    final Authentication mockAuthentication = Mockito.mock(Authentication.class);
    final SettingsProvider settingsProvider = new SettingsProvider(mockClient, mockAuthentication);

    final GlobalSettingsInfo globalSettingsInfo = settingsProvider.getGlobalSettings();

    // Simply verify that global settings has been returned, and that the correct APIs were invoked.
    verifySettings(globalSettingsInfo);
  }

  @Test
  public void testGetGlobalSettingsFailure() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockClient.getV2(
        Mockito.eq(GLOBAL_SETTINGS_ENTITY_NAME),
        Mockito.eq(GLOBAL_SETTINGS_URN),
        Mockito.eq(ImmutableSet.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    )).thenThrow(
        new RemoteInvocationException()
    );

    final Authentication mockAuthentication = Mockito.mock(Authentication.class);
    final SettingsProvider settingsProvider = new SettingsProvider(mockClient, mockAuthentication);

    // Simply verify that global settings has been returned, and that the correct APIs were invoked.
    Assert.assertThrows(RuntimeException.class, settingsProvider::getGlobalSettings);
  }

  private void verifySettings(final GlobalSettingsInfo globalSettings) {
    Assert.assertEquals(globalSettings, mockSettings());
  }

  private EntityResponse mockSettingsResponse() {
    final EntityResponse user = new EntityResponse();
    user.setUrn(GLOBAL_SETTINGS_URN);
    user.setEntityName(GLOBAL_SETTINGS_ENTITY_NAME);
    final EnvelopedAspectMap globalSettingsAspects = new EnvelopedAspectMap();
    globalSettingsAspects.put(GLOBAL_SETTINGS_INFO_ASPECT_NAME, new EnvelopedAspect()
        .setName(GLOBAL_SETTINGS_INFO_ASPECT_NAME)
        .setType(AspectType.VERSIONED)
        .setCreated(mockAuditStamp())
        .setValue(new Aspect(
            mockSettings().data()
        ))
    );
    user.setAspects(globalSettingsAspects);
    return user;
  }

  private GlobalSettingsInfo mockSettings() {
    return new GlobalSettingsInfo()
        .setIntegrations(new GlobalIntegrationSettings().setSlackSettings(new SlackIntegrationSettings().setEnabled(true)))
        .setNotifications(new GlobalNotificationSettings());
  }

  private AuditStamp mockAuditStamp() {
    return new AuditStamp().setActor(Urn.createFromTuple(CORP_USER_ENTITY_NAME, "test")).setTime(0L);
  }
}