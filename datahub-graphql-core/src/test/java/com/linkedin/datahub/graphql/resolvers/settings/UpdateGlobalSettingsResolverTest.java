package com.linkedin.datahub.graphql.resolvers.settings;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.NotificationScenarioType;
import com.linkedin.datahub.graphql.generated.NotificationSettingInput;
import com.linkedin.datahub.graphql.generated.NotificationSettingValue;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.UpdateGlobalIntegrationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGlobalNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGlobalSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateOidcSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateSlackIntegrationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateSsoSettingsInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.settings.NotificationSettingMap;
import com.linkedin.settings.global.GlobalIntegrationSettings;
import com.linkedin.settings.global.GlobalNotificationSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.SlackIntegrationSettings;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.testng.Assert.*;


public class UpdateGlobalSettingsResolverTest {
  private static final String BASE_URL_VALUE = "http://localhost:9002";
  private static final String CLIENT_ID_VALUE = "clientId";
  private static final String CLIENT_SECRET_VALUE = "clientSecret";
  private static final String DISCOVERY_URI_VALUE = "https://idp.com/.well-known/openid-configuration";
  private static final UpdateGlobalSettingsInput TEST_INPUT = new UpdateGlobalSettingsInput();

  @BeforeMethod
  public void setUp() {
    TEST_INPUT.setIntegrationSettings(
        new UpdateGlobalIntegrationSettingsInput(new UpdateSlackIntegrationSettingsInput(true, "channel", "token")));
    TEST_INPUT.setNotificationSettings(new UpdateGlobalNotificationSettingsInput(ImmutableList.of(
        new NotificationSettingInput(NotificationScenarioType.DATASET_SCHEMA_CHANGE, NotificationSettingValue.ENABLED,
            ImmutableList.of(new StringMapEntryInput("key", "value"))))));

    final UpdateSsoSettingsInput updateSsoSettingsInput = new UpdateSsoSettingsInput();
    updateSsoSettingsInput.setBaseUrl(BASE_URL_VALUE);

    final UpdateOidcSettingsInput updateOidcSettingsInput = new UpdateOidcSettingsInput();
    updateOidcSettingsInput.setEnabled(true);
    updateOidcSettingsInput.setClientId(CLIENT_ID_VALUE);
    updateOidcSettingsInput.setClientSecret(CLIENT_SECRET_VALUE);
    updateOidcSettingsInput.setDiscoveryUri(DISCOVERY_URI_VALUE);
    updateSsoSettingsInput.setOidcSettings(updateOidcSettingsInput);

    TEST_INPUT.setSsoSettings(updateSsoSettingsInput);
  }

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SecretService mockSecretService = Mockito.mock(SecretService.class);

    GlobalSettingsInfo returnedInfo = getGlobalSettingsInfo();

    Mockito.when(mockClient.getV2(
        Mockito.eq(Constants.GLOBAL_SETTINGS_ENTITY_NAME),
        Mockito.eq(Constants.GLOBAL_SETTINGS_URN),
        Mockito.eq(ImmutableSet.of(
            Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME)),
        Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setEntityName(Constants.GLOBAL_SETTINGS_ENTITY_NAME)
            .setUrn(Constants.GLOBAL_SETTINGS_URN)
            .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(returnedInfo.data()))
                    .setCreated(new AuditStamp()
                        .setTime(0L)
                        .setActor(Urn.createFromString("urn:li:corpuser:test"))
                    )))));

    Mockito.when(mockSecretService.encrypt("token")).thenReturn("token");
    Mockito.when(mockSecretService.encrypt(CLIENT_SECRET_VALUE)).thenReturn(CLIENT_SECRET_VALUE);

    UpdateGlobalSettingsResolver resolver = new UpdateGlobalSettingsResolver(mockClient, mockSecretService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    MetadataChangeProposal expectedProposal = new MetadataChangeProposal();
    expectedProposal.setEntityUrn(Constants.GLOBAL_SETTINGS_URN);
    expectedProposal.setChangeType(ChangeType.UPSERT);
    expectedProposal.setAspectName(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    expectedProposal.setEntityType(Constants.GLOBAL_SETTINGS_ENTITY_NAME);
    expectedProposal.setAspect(GenericRecordUtils.serializeAspect(returnedInfo));

    Mockito.verify(mockClient, Mockito.times(1)).ingestProposal(
        Mockito.eq(expectedProposal),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SecretService mockSecretService = Mockito.mock(SecretService.class);

    UpdateGlobalSettingsResolver resolver = new UpdateGlobalSettingsResolver(mockClient, mockSecretService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SecretService mockSecretService = Mockito.mock(SecretService.class);

    Mockito.doThrow(RemoteInvocationException.class).when(mockClient).ingestProposal(
        Mockito.any(),
        Mockito.any(Authentication.class));

    UpdateGlobalSettingsResolver resolver = new UpdateGlobalSettingsResolver(mockClient, mockSecretService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }

  public static GlobalSettingsInfo getGlobalSettingsInfo() {
    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();
    globalSettingsInfo.setIntegrations(new GlobalIntegrationSettings()
        .setSlackSettings(new SlackIntegrationSettings()
            .setEnabled(true)
            .setDefaultChannelName("test")
        )
    );
    NotificationSettingMap map = new NotificationSettingMap();
    map.put(NotificationScenarioType.INGESTION_RUN_CHANGE.toString(),
        new com.linkedin.settings.NotificationSetting().setValue(com.linkedin.settings.NotificationSettingValue.ENABLED));
    map.put(NotificationScenarioType.ENTITY_DEPRECATION_CHANGE.toString(),
        new com.linkedin.settings.NotificationSetting().setValue(com.linkedin.settings.NotificationSettingValue.DISABLED));
    globalSettingsInfo.setNotifications(new GlobalNotificationSettings().setSettings(map));
    return globalSettingsInfo;
  }
}

