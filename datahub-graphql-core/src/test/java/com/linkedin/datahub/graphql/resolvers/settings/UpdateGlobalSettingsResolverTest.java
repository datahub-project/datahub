package com.linkedin.datahub.graphql.resolvers.settings;

import static com.linkedin.datahub.graphql.TestUtils.verifyIngestProposal;
import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

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
import com.linkedin.datahub.graphql.generated.TeamsChannelInput;
import com.linkedin.datahub.graphql.generated.UpdateDocumentationAiSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateEmailIntegrationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGlobalIntegrationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGlobalNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGlobalSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateOidcSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateSlackIntegrationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateSsoSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateTeamsIntegrationSettingsInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.settings.NotificationSettingMap;
import com.linkedin.settings.global.DocumentationAiSettings;
import com.linkedin.settings.global.EmailIntegrationSettings;
import com.linkedin.settings.global.GlobalIntegrationSettings;
import com.linkedin.settings.global.GlobalNotificationSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.SlackIntegrationSettings;
import com.linkedin.settings.global.TeamsChannel;
import com.linkedin.settings.global.TeamsIntegrationSettings;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.util.concurrent.ExecutionException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateGlobalSettingsResolverTest {
  private static final String BASE_URL_VALUE = "http://localhost:9002";
  private static final String CLIENT_ID_VALUE = "clientId";
  private static final String CLIENT_SECRET_VALUE = "clientSecret";
  private static final String DISCOVERY_URI_VALUE =
      "https://idp.com/.well-known/openid-configuration";
  private static final String PREFERRED_JWS_ALGORITHM = "jws1";
  private static final UpdateGlobalSettingsInput TEST_INPUT = new UpdateGlobalSettingsInput();

  @BeforeMethod
  public void setUp() {
    TEST_INPUT.setIntegrationSettings(
        new UpdateGlobalIntegrationSettingsInput(
            new UpdateSlackIntegrationSettingsInput("channel", "token", true),
            new UpdateEmailIntegrationSettingsInput("test@test.com"),
            null));
    TEST_INPUT.setNotificationSettings(
        new UpdateGlobalNotificationSettingsInput(
            ImmutableList.of(
                new NotificationSettingInput(
                    NotificationScenarioType.DATASET_SCHEMA_CHANGE,
                    NotificationSettingValue.ENABLED,
                    ImmutableList.of(new StringMapEntryInput("key", "value"))))));

    final UpdateSsoSettingsInput updateSsoSettingsInput = new UpdateSsoSettingsInput();
    updateSsoSettingsInput.setBaseUrl(BASE_URL_VALUE);

    final UpdateOidcSettingsInput updateOidcSettingsInput = new UpdateOidcSettingsInput();
    updateOidcSettingsInput.setEnabled(true);
    updateOidcSettingsInput.setClientId(CLIENT_ID_VALUE);
    updateOidcSettingsInput.setClientSecret(CLIENT_SECRET_VALUE);
    updateOidcSettingsInput.setDiscoveryUri(DISCOVERY_URI_VALUE);
    updateOidcSettingsInput.setPreferredJwsAlgorithm(PREFERRED_JWS_ALGORITHM);
    updateSsoSettingsInput.setOidcSettings(updateOidcSettingsInput);

    TEST_INPUT.setSsoSettings(updateSsoSettingsInput);

    UpdateDocumentationAiSettingsInput docAiInput = new UpdateDocumentationAiSettingsInput();
    docAiInput.setEnabled(true);
    TEST_INPUT.setDocumentationAi(docAiInput);
  }

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

    Mockito.when(mockSecretService.encrypt("token")).thenReturn("token");
    Mockito.when(mockSecretService.encrypt(CLIENT_SECRET_VALUE)).thenReturn(CLIENT_SECRET_VALUE);

    IntegrationsService mockIntegrationsService = Mockito.mock(IntegrationsService.class);

    UpdateGlobalSettingsResolver resolver =
        new UpdateGlobalSettingsResolver(mockClient, mockSecretService, mockIntegrationsService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    MetadataChangeProposal expectedProposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            GLOBAL_SETTINGS_URN, GLOBAL_SETTINGS_INFO_ASPECT_NAME, returnedInfo);
    expectedProposal.setEntityUrn(Constants.GLOBAL_SETTINGS_URN);
    expectedProposal.setChangeType(ChangeType.UPSERT);
    expectedProposal.setAspectName(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    expectedProposal.setEntityType(Constants.GLOBAL_SETTINGS_ENTITY_NAME);
    expectedProposal.setAspect(GenericRecordUtils.serializeAspect(returnedInfo));

    verifyIngestProposal(mockClient, 1, expectedProposal);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SecretService mockSecretService = Mockito.mock(SecretService.class);
    IntegrationsService mockIntegrationsService = Mockito.mock(IntegrationsService.class);

    UpdateGlobalSettingsResolver resolver =
        new UpdateGlobalSettingsResolver(mockClient, mockSecretService, mockIntegrationsService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), Mockito.any());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SecretService mockSecretService = Mockito.mock(SecretService.class);

    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .ingestProposal(any(OperationContext.class), Mockito.any());

    IntegrationsService mockIntegrationsService = Mockito.mock(IntegrationsService.class);

    UpdateGlobalSettingsResolver resolver =
        new UpdateGlobalSettingsResolver(mockClient, mockSecretService, mockIntegrationsService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testUpdateGlobalSettings_TeamsSettings() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SecretService mockSecretService = Mockito.mock(SecretService.class);
    IntegrationsService mockIntegrationsService = Mockito.mock(IntegrationsService.class);

    // Setup Teams integration settings
    UpdateGlobalIntegrationSettingsInput integrationSettings =
        new UpdateGlobalIntegrationSettingsInput();

    TeamsChannelInput teamsChannel = new TeamsChannelInput();
    teamsChannel.setId("channel-123");
    teamsChannel.setName("General");

    UpdateTeamsIntegrationSettingsInput teamsSettings = new UpdateTeamsIntegrationSettingsInput();
    teamsSettings.setDefaultChannel(teamsChannel);

    integrationSettings.setTeamsSettings(teamsSettings);

    UpdateGlobalSettingsInput input = new UpdateGlobalSettingsInput();
    input.setIntegrationSettings(integrationSettings);

    GlobalSettingsInfo returnedInfo = getGlobalSettingsInfoWithTeams();

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

    UpdateGlobalSettingsResolver resolver =
        new UpdateGlobalSettingsResolver(mockClient, mockSecretService, mockIntegrationsService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean result = resolver.get(mockEnv).get();

    // Verify
    assertTrue(result);
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.eq(false));
  }

  @Test
  public void testUpdateGlobalSettings_TeamsSettingsWithoutChannel() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SecretService mockSecretService = Mockito.mock(SecretService.class);
    IntegrationsService mockIntegrationsService = Mockito.mock(IntegrationsService.class);

    // Setup Teams integration settings without default channel
    UpdateGlobalIntegrationSettingsInput integrationSettings =
        new UpdateGlobalIntegrationSettingsInput();

    UpdateTeamsIntegrationSettingsInput teamsSettings = new UpdateTeamsIntegrationSettingsInput();
    // No default channel set

    integrationSettings.setTeamsSettings(teamsSettings);

    UpdateGlobalSettingsInput input = new UpdateGlobalSettingsInput();
    input.setIntegrationSettings(integrationSettings);

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

    UpdateGlobalSettingsResolver resolver =
        new UpdateGlobalSettingsResolver(mockClient, mockSecretService, mockIntegrationsService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean result = resolver.get(mockEnv).get();

    // Verify
    assertTrue(result);
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.eq(false));
  }

  @Test
  public void testUpdateGlobalSettings_InvalidInput() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SecretService mockSecretService = Mockito.mock(SecretService.class);
    IntegrationsService mockIntegrationsService = Mockito.mock(IntegrationsService.class);

    UpdateGlobalSettingsResolver resolver =
        new UpdateGlobalSettingsResolver(mockClient, mockSecretService, mockIntegrationsService);

    // Execute resolver with null input
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(null);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());

    // Verify no ingestion calls were made
    Mockito.verify(mockClient, Mockito.never())
        .ingestProposal(
            any(OperationContext.class),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.anyBoolean());
  }

  public static GlobalSettingsInfo getGlobalSettingsInfoWithTeams() {
    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    TeamsChannel teamsChannel = new TeamsChannel();
    teamsChannel.setId("channel-123");
    teamsChannel.setName("General");

    TeamsIntegrationSettings teamsSettings = new TeamsIntegrationSettings();
    teamsSettings.setEnabled(true);
    teamsSettings.setDefaultChannel(teamsChannel);

    globalSettingsInfo.setIntegrations(
        new GlobalIntegrationSettings()
            .setSlackSettings(
                new SlackIntegrationSettings().setEnabled(true).setDefaultChannelName("test"))
            .setEmailSettings(new EmailIntegrationSettings().setDefaultEmail("test@test.com"))
            .setTeamsSettings(teamsSettings));

    NotificationSettingMap map = new NotificationSettingMap();
    map.put(
        NotificationScenarioType.INGESTION_RUN_CHANGE.toString(),
        new com.linkedin.settings.NotificationSetting()
            .setValue(com.linkedin.settings.NotificationSettingValue.ENABLED));
    map.put(
        NotificationScenarioType.ENTITY_DEPRECATION_CHANGE.toString(),
        new com.linkedin.settings.NotificationSetting()
            .setValue(com.linkedin.settings.NotificationSettingValue.DISABLED));
    globalSettingsInfo.setNotifications(new GlobalNotificationSettings().setSettings(map));
    globalSettingsInfo.setDocumentationAi(new DocumentationAiSettings().setEnabled(true));
    return globalSettingsInfo;
  }

  public static GlobalSettingsInfo getGlobalSettingsInfo() {
    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();
    globalSettingsInfo.setIntegrations(
        new GlobalIntegrationSettings()
            .setSlackSettings(
                new SlackIntegrationSettings()
                    .setEnabled(true)
                    .setDefaultChannelName("test")
                    .setDatahubAtMentionEnabled(true))
            .setEmailSettings(new EmailIntegrationSettings().setDefaultEmail("test@test.com")));
    NotificationSettingMap map = new NotificationSettingMap();
    map.put(
        NotificationScenarioType.INGESTION_RUN_CHANGE.toString(),
        new com.linkedin.settings.NotificationSetting()
            .setValue(com.linkedin.settings.NotificationSettingValue.ENABLED));
    map.put(
        NotificationScenarioType.ENTITY_DEPRECATION_CHANGE.toString(),
        new com.linkedin.settings.NotificationSetting()
            .setValue(com.linkedin.settings.NotificationSettingValue.DISABLED));
    globalSettingsInfo.setNotifications(new GlobalNotificationSettings().setSettings(map));
    globalSettingsInfo.setDocumentationAi(new DocumentationAiSettings().setEnabled(true));
    return globalSettingsInfo;
  }
}
