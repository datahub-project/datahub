package com.linkedin.datahub.graphql.resolvers.settings;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GlobalSettings;
import com.linkedin.datahub.graphql.generated.NotificationScenarioType;
import com.linkedin.datahub.graphql.generated.NotificationSetting;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.settings.NotificationSettingMap;
import com.linkedin.settings.NotificationSettingValue;
import com.linkedin.settings.global.EmailIntegrationSettings;
import com.linkedin.settings.global.GlobalIntegrationSettings;
import com.linkedin.settings.global.GlobalNotificationSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.SlackIntegrationSettings;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class GlobalSettingsResolverTest {

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SecretService mockSecretService = Mockito.mock(SecretService.class);

    GlobalSettingsInfo returnedInfo = getGlobalSettingsInfo();

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.GLOBAL_SETTINGS_ENTITY_NAME),
                Mockito.eq(Constants.GLOBAL_SETTINGS_URN),
                Mockito.eq(ImmutableSet.of(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setEntityName(Constants.GLOBAL_SETTINGS_ENTITY_NAME)
                .setUrn(Constants.GLOBAL_SETTINGS_URN)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(returnedInfo.data()))
                                .setCreated(
                                    new AuditStamp()
                                        .setTime(0L)
                                        .setActor(
                                            Urn.createFromString("urn:li:corpuser:test")))))));

    GlobalSettingsResolver resolver = new GlobalSettingsResolver(mockClient, mockSecretService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Data Assertions
    verifyGlobalSettingsResult(resolver.get(mockEnv).get(), returnedInfo);
  }

  public static void verifyGlobalSettingsResult(
      GlobalSettings actual, GlobalSettingsInfo expected) {

    // Verify integration settings.
    assertEquals(
        actual.getIntegrationSettings().getSlackSettings().getDefaultChannelName(),
        expected.getIntegrations().getSlackSettings().getDefaultChannelName());

    assertEquals(
        actual.getIntegrationSettings().getEmailSettings().getDefaultEmail(),
        expected.getIntegrations().getEmailSettings().getDefaultEmail());

    // Verify notification settings settings.
    for (NotificationSetting actualSetting : actual.getNotificationSettings().getSettings()) {
      assertEquals(
          actualSetting.getValue().toString(),
          expected
              .getNotifications()
              .getSettings()
              .get(actualSetting.getType().toString())
              .getValue()
              .toString());
    }
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SecretService mockSecretService = Mockito.mock(SecretService.class);
    GlobalSettingsResolver resolver = new GlobalSettingsResolver(mockClient, mockSecretService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .batchGetV2(any(OperationContext.class), Mockito.any(), Mockito.anySet(), Mockito.anySet());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(OperationContext.class), Mockito.any(), Mockito.anySet(), Mockito.anySet());
    SecretService mockSecretService = Mockito.mock(SecretService.class);

    GlobalSettingsResolver resolver = new GlobalSettingsResolver(mockClient, mockSecretService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }

  public static GlobalSettingsInfo getGlobalSettingsInfo() {
    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();
    globalSettingsInfo.setIntegrations(
        new GlobalIntegrationSettings()
            .setSlackSettings(
                new SlackIntegrationSettings().setEnabled(true).setDefaultChannelName("test"))
            .setEmailSettings(new EmailIntegrationSettings().setDefaultEmail("test@test.com")));
    NotificationSettingMap map = new NotificationSettingMap();
    map.put(
        NotificationScenarioType.INGESTION_RUN_CHANGE.toString(),
        new com.linkedin.settings.NotificationSetting().setValue(NotificationSettingValue.ENABLED));
    map.put(
        NotificationScenarioType.ENTITY_DEPRECATION_CHANGE.toString(),
        new com.linkedin.settings.NotificationSetting()
            .setValue(NotificationSettingValue.DISABLED));
    globalSettingsInfo.setNotifications(new GlobalNotificationSettings().setSettings(map));
    return globalSettingsInfo;
  }
}
