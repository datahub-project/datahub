package com.datahub.notification.slack;

import static com.linkedin.metadata.AcrylConstants.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.notification.NotificationContext;
import com.datahub.notification.NotificationSinkConfig;
import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.provider.EntityNameProvider;
import com.datahub.notification.provider.IdentityProvider;
import com.datahub.notification.provider.SecretProvider;
import com.datahub.notification.provider.SettingsProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.connection.DataHubConnectionDetails;
import com.linkedin.connection.DataHubConnectionDetailsType;
import com.linkedin.connection.DataHubJsonConnection;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.NotificationMessage;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientArray;
import com.linkedin.event.notification.NotificationRecipientOriginType;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.StructuredExecutionReport;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.connection.ConnectionService;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.global.GlobalIntegrationSettings;
import com.linkedin.settings.global.GlobalNotificationSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.SlackIntegrationSettings;
import com.slack.api.Slack;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.methods.request.users.UsersLookupByEmailRequest;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.methods.response.users.UsersLookupByEmailResponse;
import com.slack.api.model.User;
import io.datahubproject.integrations.invoker.JSON;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import okhttp3.Response;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class SlackNotificationSinkTest {

  private static final Urn TEST_USER_URN = Urn.createFromTuple(CORP_USER_ENTITY_NAME, "test");
  private static final String TEST_BASE_URL = "http://localhost:9002";
  private static final String TEST_BOT_TOKEN = "abc";
  private static final String TEST_EXECUTION_REQUEST_URN =
      Urn.createFromTuple(EXECUTION_REQUEST_ENTITY_NAME, "test").toString();
  private OperationContext opContext;

  private EntityClient mockEntityClient;

  private SlackNotificationSink sink;

  @Mock private FeatureFlags mockFeatureFlags;

  @BeforeTest
  public void setup() {
    MockitoAnnotations.openMocks(this);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    mockEntityClient = mock(EntityClient.class);
    Slack mockSlack = mock(Slack.class);
    Mockito.when(mockSlack.methods(anyString())).thenReturn(mock(MethodsClient.class));

    // Configure mock sink.
    sink = new SlackNotificationSink(mockSlack);
    sink.slackClient = mock(MethodsClient.class);

    SettingsProvider mockSettingsProvider = mock(SettingsProvider.class);
    IdentityProvider mockIdentityProvider = mock(IdentityProvider.class);
    EntityNameProvider mockEntityNameProvider = Mockito.mock(EntityNameProvider.class);
    SecretProvider mockSecretProvider = mock(SecretProvider.class);
    IntegrationsService mockIntegrationsService = mock(IntegrationsService.class);
    ConnectionService mockConnectionService = mock(ConnectionService.class);
    mockConnectionService = mock(ConnectionService.class);
    Mockito.when(
            mockConnectionService.getConnectionDetails(any(OperationContext.class), any(Urn.class)))
        .thenReturn(null);

    sink.init(
        new NotificationSinkConfig(
            ImmutableMap.of("botToken", TEST_BOT_TOKEN, "defaultChannel", "#test"),
            mockEntityClient,
            mockSettingsProvider,
            mockIdentityProvider,
            mockEntityNameProvider,
            mockSecretProvider,
            mockConnectionService,
            mockIntegrationsService,
            TEST_BASE_URL));
  }

  @Test
  public void testInit() throws Exception {
    // Verify that init doesn't throw when empty configs are provided,
    // and when populated configs are provided.
    SettingsProvider mockSettingsProvider = mock(SettingsProvider.class);
    IdentityProvider mockIdentityProvider = mock(IdentityProvider.class);
    EntityNameProvider mockEntityNameProvider = Mockito.mock(EntityNameProvider.class);
    SecretProvider mockSecretProvider = mock(SecretProvider.class);
    IntegrationsService mockIntegrationsService = mock(IntegrationsService.class);

    SlackNotificationSink sink = new SlackNotificationSink();

    ConnectionService mockConnectionService = mock(ConnectionService.class);
    mockConnectionService = mock(ConnectionService.class);
    Mockito.when(
            mockConnectionService.getConnectionDetails(any(OperationContext.class), any(Urn.class)))
        .thenReturn(null);

    // Case 1: No Connection, Empty Static Config
    sink.init(
        new NotificationSinkConfig(
            Collections.emptyMap(),
            mockEntityClient,
            mockSettingsProvider,
            mockIdentityProvider,
            mockEntityNameProvider,
            mockSecretProvider,
            mockConnectionService,
            mockIntegrationsService,
            TEST_BASE_URL));

    // Case 2: No Connection, Static config with bot token and default channel
    sink.init(
        new NotificationSinkConfig(
            ImmutableMap.of(
                "botToken",
                "abc",
                "defaultChannel",
                "#test",
                "retryEnabled",
                "true",
                "proxyUrl",
                "https://proxy.acryl.io"),
            mockEntityClient,
            mockSettingsProvider,
            mockIdentityProvider,
            mockEntityNameProvider,
            mockSecretProvider,
            mockConnectionService,
            mockIntegrationsService,
            TEST_BASE_URL));
  }

  @Test
  public void testIsEnabled() {

    // Case 1: Slack configs are stored in new connection object
    SettingsProvider mockSettingsProvider = mock(SettingsProvider.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings(opContext)).thenReturn(enabledSettings());
    ConnectionService mockConnectionService = mock(ConnectionService.class);
    Mockito.when(
            mockConnectionService.getConnectionDetails(
                any(OperationContext.class), eq(SlackNotificationSink.SLACK_CONNECTION_URN)))
        .thenReturn(
            new DataHubConnectionDetails()
                .setType(DataHubConnectionDetailsType.JSON)
                .setJson(new DataHubJsonConnection().setEncryptedBlob("blob")));
    IdentityProvider mockIdentityProvider = mock(IdentityProvider.class);
    Mockito.when(mockIdentityProvider.getUser(any(OperationContext.class), eq(TEST_USER_URN)))
        .thenReturn(testUser());
    EntityNameProvider mockEntityNameProvider = Mockito.mock(EntityNameProvider.class);
    SecretProvider mockSecretProvider = mock(SecretProvider.class);
    IntegrationsService mockIntegrationsService = mock(IntegrationsService.class);

    Mockito.when(mockSecretProvider.decryptSecret("blob"))
        .thenReturn("{\"bot_token\": \"test-token\"}");

    Slack mockSlack = mock(Slack.class);
    Mockito.when(mockSlack.methods(anyString())).thenReturn(mock(MethodsClient.class));

    SlackNotificationSink sink = new SlackNotificationSink(mockSlack);

    sink.init(
        new NotificationSinkConfig(
            ImmutableMap.of("botToken", TEST_BOT_TOKEN, "defaultChannel", "#test"),
            mockEntityClient,
            mockSettingsProvider,
            mockIdentityProvider,
            mockEntityNameProvider,
            mockSecretProvider,
            mockConnectionService,
            mockIntegrationsService,
            TEST_BASE_URL));

    assertTrue(sink.isEnabled(opContext));
    Assert.assertEquals(sink.botToken, "test-token");

    // Case 2: Slack configs are stored in legacy settings.
    mockConnectionService = mock(ConnectionService.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings(opContext)).thenReturn(enabledSettings());
    Mockito.when(
            mockConnectionService.getConnectionDetails(
                any(OperationContext.class), eq(SlackNotificationSink.SLACK_CONNECTION_URN)))
        .thenReturn(
            new DataHubConnectionDetails()
                .setType(DataHubConnectionDetailsType.JSON)
                .setJson(new DataHubJsonConnection().setEncryptedBlob("blob")));
    // No bot_token field in the json
    Mockito.when(mockSecretProvider.decryptSecret("blob")).thenReturn("{}");
    sink.init(
        new NotificationSinkConfig(
            ImmutableMap.of("botToken", TEST_BOT_TOKEN, "defaultChannel", "#test"),
            mockEntityClient,
            mockSettingsProvider,
            mockIdentityProvider,
            mockEntityNameProvider,
            mockSecretProvider,
            mockConnectionService,
            mockIntegrationsService,
            TEST_BASE_URL));

    assertTrue(sink.isEnabled(opContext));
    Assert.assertEquals(sink.botToken, TEST_BOT_TOKEN);

    // Case 3: Slack configs are stored in static configs
    mockSettingsProvider = mock(SettingsProvider.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings(opContext)).thenReturn(emptySettings());
    mockConnectionService = mock(ConnectionService.class);
    Mockito.when(
            mockConnectionService.getConnectionDetails(
                any(OperationContext.class), eq(SlackNotificationSink.SLACK_CONNECTION_URN)))
        .thenReturn(null);
    sink.init(
        new NotificationSinkConfig(
            ImmutableMap.of("botToken", TEST_BOT_TOKEN, "defaultChannel", "#test"),
            mockEntityClient,
            mockSettingsProvider,
            mockIdentityProvider,
            mockEntityNameProvider,
            mockSecretProvider,
            mockConnectionService,
            mockIntegrationsService,
            TEST_BASE_URL));

    assertTrue(sink.isEnabled(opContext));
    Assert.assertEquals(sink.botToken, TEST_BOT_TOKEN);

    // Case 4: Changing the connection details at runtime should change the bot token and slack
    // client.
    Mockito.when(mockSettingsProvider.getGlobalSettings(opContext)).thenReturn(enabledSettings());
    Mockito.when(
            mockConnectionService.getConnectionDetails(
                any(OperationContext.class), eq(SlackNotificationSink.SLACK_CONNECTION_URN)))
        .thenReturn(
            new DataHubConnectionDetails()
                .setType(DataHubConnectionDetailsType.JSON)
                .setJson(new DataHubJsonConnection().setEncryptedBlob("blob")));
    Mockito.when(mockSecretProvider.decryptSecret("blob"))
        .thenReturn("{\"bot_token\": \"test-token\"}");

    assertTrue(sink.isEnabled(opContext));
    Assert.assertEquals(sink.botToken, "test-token");
    MethodsClient methodsClient1 = sink.slackClient;

    // Changing the bot token
    Mockito.when(
            mockConnectionService.getConnectionDetails(
                any(OperationContext.class), eq(SlackNotificationSink.SLACK_CONNECTION_URN)))
        .thenReturn(
            new DataHubConnectionDetails()
                .setType(DataHubConnectionDetailsType.JSON)
                .setJson(new DataHubJsonConnection().setEncryptedBlob("blob")));
    Mockito.when(mockSecretProvider.decryptSecret("blob"))
        .thenReturn("{\"bot_token\": \"test-token-2\"}");

    assertTrue(sink.isEnabled(opContext));
    Assert.assertEquals(sink.botToken, "test-token-2"); // Bot token was updated.
    MethodsClient methodsClient2 = sink.slackClient;

    Assert.assertNotEquals(
        methodsClient1, methodsClient2); // Different clients were used for different bot tokens.
  }

  @Test
  public void testSendEnabledMultipleRecipientTypes() throws Exception {

    /*
     * This test verifies that sending a notification of type "CUSTOM"
     * works for a specific user, a custom channel, and the default configured
     * channel works as expected in the success case.
     */

    SettingsProvider mockSettingsProvider = mock(SettingsProvider.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings(opContext)).thenReturn(enabledSettings());
    ConnectionService mockConnectionService = mock(ConnectionService.class);
    Mockito.when(
            mockConnectionService.getConnectionDetails(any(OperationContext.class), any(Urn.class)))
        .thenReturn(null);
    IdentityProvider mockIdentityProvider = mock(IdentityProvider.class);
    Mockito.when(mockIdentityProvider.getUser(any(OperationContext.class), eq(TEST_USER_URN)))
        .thenReturn(testUser());
    EntityNameProvider mockEntityNameProvider = Mockito.mock(EntityNameProvider.class);
    SecretProvider mockSecretProvider = mock(SecretProvider.class);
    IntegrationsService mockIntegrationsService = mock(IntegrationsService.class);

    // Init the slack client mock
    MethodsClient mockSlackClient = mock(MethodsClient.class);
    UsersLookupByEmailRequest lookupRequests =
        UsersLookupByEmailRequest.builder().email("test@gmail.com").build();
    UsersLookupByEmailResponse lookupResponse = new UsersLookupByEmailResponse();
    lookupResponse.setOk(true);
    User slackUser = new User();
    slackUser.setId("test-id");
    lookupResponse.setUser(slackUser);
    Mockito.when(mockSlackClient.usersLookupByEmail(eq(lookupRequests))).thenReturn(lookupResponse);

    ChatPostMessageRequest userMsgRequest =
        ChatPostMessageRequest.builder()
            .channel("test-id")
            .text("*Test Message Title*\nTest Message Body")
            .iconUrl(String.format("http://localhost:9002%s", ACRYL_LOGO_FILE_PATH))
            .build();
    ChatPostMessageResponse usrMsgResponse = new ChatPostMessageResponse();
    usrMsgResponse.setOk(true);
    Mockito.when(mockSlackClient.chatPostMessage(eq(userMsgRequest))).thenReturn(usrMsgResponse);

    ChatPostMessageRequest channelMsgRequest =
        ChatPostMessageRequest.builder()
            .channel("#test-channel")
            .text("*Test Message Title*\nTest Message Body")
            .iconUrl(String.format("http://localhost:9002%s", ACRYL_LOGO_FILE_PATH))
            .build();
    ChatPostMessageResponse channelMsgResponse = new ChatPostMessageResponse();
    channelMsgResponse.setOk(true);
    Mockito.when(mockSlackClient.chatPostMessage(eq(channelMsgRequest)))
        .thenReturn(channelMsgResponse);

    ChatPostMessageRequest defaultChannelMsgRequest =
        ChatPostMessageRequest.builder()
            .channel("#test")
            .text("*Test Message Title*\nTest Message Body")
            .iconUrl(String.format("http://localhost:9002%s", ACRYL_LOGO_FILE_PATH))
            .build();
    ChatPostMessageResponse defaultChannelMsgResponse = new ChatPostMessageResponse();
    defaultChannelMsgResponse.setOk(true);
    Mockito.when(mockSlackClient.chatPostMessage(eq(defaultChannelMsgRequest)))
        .thenReturn(defaultChannelMsgResponse);

    // Init with a mock slack client.
    SlackNotificationSink sink = new SlackNotificationSink(mockSlackClient);
    sink.botToken = TEST_BOT_TOKEN;
    sink.init(
        new NotificationSinkConfig(
            ImmutableMap.of("botToken", TEST_BOT_TOKEN, "defaultChannel", "#test"),
            mockEntityClient,
            mockSettingsProvider,
            mockIdentityProvider,
            mockEntityNameProvider,
            mockSecretProvider,
            mockConnectionService,
            mockIntegrationsService,
            TEST_BASE_URL));

    // Now, construct a message request and verify that it is sent to the slack client.
    NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(
        new NotificationMessage()
            .setTemplate(
                com.linkedin.event.notification.template.NotificationTemplateType.valueOf(
                    NotificationTemplateType.CUSTOM.name()))
            .setParameters(
                new StringMap(
                    ImmutableMap.of("title", "Test Message Title", "body", "Test Message Body"))));

    // Send to a user and a custom channel.
    notificationRequest.setRecipients(
        new NotificationRecipientArray(
            ImmutableList.of(
                // User recipient
                new NotificationRecipient()
                    .setType(NotificationRecipientType.USER)
                    .setId(TEST_USER_URN.toString()),
                // Custom Channel Recipient
                new NotificationRecipient()
                    .setType(NotificationRecipientType.SLACK_CHANNEL)
                    .setId("#test-channel"),
                // Default Channel Recipient
                new NotificationRecipient().setType(NotificationRecipientType.SLACK_CHANNEL))));

    // Send the request
    sink.send(opContext, notificationRequest, new NotificationContext());

    // Verify that the messages have been sent via the client as expected.
    Mockito.verify(mockSlackClient, Mockito.times(1)).usersLookupByEmail(eq(lookupRequests));

    Mockito.verify(mockSlackClient, Mockito.times(1)).chatPostMessage(eq(userMsgRequest));

    Mockito.verify(mockSlackClient, Mockito.times(1)).chatPostMessage(eq(channelMsgRequest));

    Mockito.verify(mockSlackClient, Mockito.times(1)).chatPostMessage(eq(defaultChannelMsgRequest));
  }

  @Test
  public void testSendDisabled() throws Exception {

    /*
     * This test verifies that the slack client is not used when the
     * slack sink should be disabled by global settings.
     */
    SettingsProvider mockSettingsProvider = mock(SettingsProvider.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings(any(OperationContext.class)))
        .thenReturn(disabledSettings());
    IdentityProvider mockIdentityProvider = mock(IdentityProvider.class);
    EntityNameProvider mockEntityNameProvider = Mockito.mock(EntityNameProvider.class);
    SecretProvider mockSecretProvider = mock(SecretProvider.class);
    MethodsClient mockSlackClient = mock(MethodsClient.class);
    ConnectionService mockConnectionService = mock(ConnectionService.class);
    IntegrationsService mockIntegrationsService = mock(IntegrationsService.class);

    // Init with a mock slack client.
    SlackNotificationSink sink = new SlackNotificationSink(mockSlackClient);
    sink.init(
        new NotificationSinkConfig(
            ImmutableMap.of("botToken", "abc", "defaultChannel", "#test"),
            mockEntityClient,
            mockSettingsProvider,
            mockIdentityProvider,
            mockEntityNameProvider,
            mockSecretProvider,
            mockConnectionService,
            mockIntegrationsService,
            "http://localhost:9002"));

    // Now, construct a message request and verify that it is sent to the slack client.
    NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(
        new NotificationMessage()
            .setTemplate(com.linkedin.event.notification.template.NotificationTemplateType.CUSTOM)
            .setParameters(
                new StringMap(
                    ImmutableMap.of("title", "Test Message Title", "body", "Test Message Body"))));

    // Send to a user and a custom channel.
    notificationRequest.setRecipients(
        new NotificationRecipientArray(
            ImmutableList.of(
                // User recipient
                new NotificationRecipient()
                    .setType(NotificationRecipientType.USER)
                    .setId(TEST_USER_URN.toString()))));

    // Send the request
    sink.send(opContext, notificationRequest, new NotificationContext());

    // Verify no invocations of slack client
    Mockito.verify(mockSlackClient, Mockito.times(0))
        .usersLookupByEmail(any(UsersLookupByEmailRequest.class));

    Mockito.verify(mockSlackClient, Mockito.times(0))
        .chatPostMessage(any(ChatPostMessageRequest.class));
  }

  @Test
  public void testNativeAssertionNotification() throws Exception {
    /*
     * This test verifies that sending a notification for assertion changes
     * makes requests that are expected.
     */
    SettingsProvider mockSettingsProvider = mock(SettingsProvider.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings(opContext)).thenReturn(enabledSettings());
    ConnectionService mockConnectionService = mock(ConnectionService.class);
    Mockito.when(
            mockConnectionService.getConnectionDetails(any(OperationContext.class), any(Urn.class)))
        .thenReturn(null);
    IdentityProvider mockIdentityProvider = mock(IdentityProvider.class);
    Mockito.when(mockIdentityProvider.getUser(any(OperationContext.class), eq(TEST_USER_URN)))
        .thenReturn(testUser());
    EntityNameProvider mockEntityNameProvider = Mockito.mock(EntityNameProvider.class);
    SecretProvider mockSecretProvider = mock(SecretProvider.class);
    IntegrationsService mockIntegrationsService = mock(IntegrationsService.class);

    // Init the slack client mock
    MethodsClient mockSlackClient = mock(MethodsClient.class);
    UsersLookupByEmailRequest lookupRequests =
        UsersLookupByEmailRequest.builder().email("test@gmail.com").build();
    UsersLookupByEmailResponse lookupResponse = new UsersLookupByEmailResponse();
    lookupResponse.setOk(true);
    User slackUser = new User();
    slackUser.setId("test-id");
    lookupResponse.setUser(slackUser);
    Mockito.when(mockSlackClient.usersLookupByEmail(eq(lookupRequests))).thenReturn(lookupResponse);

    ChatPostMessageRequest dmMsgRequest =
        ChatPostMessageRequest.builder()
            .channel("12345")
            .text(
                String.format(
                    ">%s  *Column Assertion* `column x is greater than y` has *passed* for *<%s|%s>*! <%s|View results>",
                    ":white_check_mark:",
                    "http://localhost:9002/datasets/test",
                    "SampleName",
                    "http://localhost:9002/datasets/test/Validation/Assertions?assertion_urn=urn%3Ali%3Aassertion%3Atest"))
            .iconUrl(String.format("http://localhost:9002%s", ACRYL_LOGO_FILE_PATH))
            .build();
    ChatPostMessageResponse defaultChannelMsgResponse = new ChatPostMessageResponse();
    defaultChannelMsgResponse.setOk(true);
    Mockito.when(mockSlackClient.chatPostMessage(eq(dmMsgRequest)))
        .thenReturn(defaultChannelMsgResponse);

    // Init with a mock slack client.
    SlackNotificationSink sink = new SlackNotificationSink(mockSlackClient);
    sink.botToken = TEST_BOT_TOKEN;
    sink.init(
        new NotificationSinkConfig(
            ImmutableMap.of("botToken", TEST_BOT_TOKEN, "defaultChannel", "#test"),
            mockEntityClient,
            mockSettingsProvider,
            mockIdentityProvider,
            mockEntityNameProvider,
            mockSecretProvider,
            mockConnectionService,
            mockIntegrationsService,
            TEST_BASE_URL));

    // Now, construct an assertion message request and verify that it is sent to the slack client.
    NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(
        new NotificationMessage()
            .setTemplate(
                com.linkedin.event.notification.template.NotificationTemplateType.valueOf(
                    NotificationTemplateType.BROADCAST_ASSERTION_STATUS_CHANGE.name()))
            .setParameters(
                new StringMap(
                    ImmutableMap.of(
                        "assertionUrn",
                        "urn:li:assertion:test",
                        "assertionType",
                        "FIELD",
                        "entityName",
                        "SampleName",
                        "entityPath",
                        "/datasets/test",
                        "result",
                        "SUCCESS",
                        "description",
                        "column x is greater than y",
                        "sourceType",
                        "NATIVE"))));

    notificationRequest.setRecipients(
        new NotificationRecipientArray(
            ImmutableList.of(
                // Custom DM recipient
                new NotificationRecipient()
                    .setType(NotificationRecipientType.SLACK_DM)
                    .setId("12345"))));

    // Send the request
    sink.send(opContext, notificationRequest, new NotificationContext());

    Mockito.verify(mockSlackClient, Mockito.times(1)).chatPostMessage(eq(dmMsgRequest));
  }

  @Test
  public void testDeprecationNotification() throws Exception {
    /*
     * This test verifies that sending a notification for deprecation updates work as expected.
     */
    SettingsProvider mockSettingsProvider = mock(SettingsProvider.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings(opContext)).thenReturn(enabledSettings());
    ConnectionService mockConnectionService = mock(ConnectionService.class);
    Mockito.when(
            mockConnectionService.getConnectionDetails(any(OperationContext.class), any(Urn.class)))
        .thenReturn(null);
    IdentityProvider mockIdentityProvider = mock(IdentityProvider.class);
    Mockito.when(mockIdentityProvider.getUser(any(OperationContext.class), eq(TEST_USER_URN)))
        .thenReturn(testUser());
    EntityNameProvider mockEntityNameProvider = Mockito.mock(EntityNameProvider.class);
    SecretProvider mockSecretProvider = mock(SecretProvider.class);
    IntegrationsService mockIntegrationsService = mock(IntegrationsService.class);

    // Init the slack client mock
    MethodsClient mockSlackClient = mock(MethodsClient.class);
    UsersLookupByEmailRequest lookupRequests =
        UsersLookupByEmailRequest.builder().email("test@gmail.com").build();
    UsersLookupByEmailResponse lookupResponse = new UsersLookupByEmailResponse();
    lookupResponse.setOk(true);
    User slackUser = new User();
    slackUser.setId("test-id");
    lookupResponse.setUser(slackUser);
    Mockito.when(mockSlackClient.usersLookupByEmail(eq(lookupRequests))).thenReturn(lookupResponse);

    ChatPostMessageRequest dmMsgRequest =
        ChatPostMessageRequest.builder()
            .channel("12345")
            .text(
                String.format(
                    ">:pencil2:  %s *<%s|%s>* has been %s by *%s*.",
                    "Dataset",
                    "http://localhost:9002/dataset/test",
                    "SampleName",
                    "marked as deprecated",
                    "Test User"))
            .iconUrl(String.format("http://localhost:9002%s", ACRYL_LOGO_FILE_PATH))
            .build();
    ChatPostMessageResponse defaultChannelMsgResponse = new ChatPostMessageResponse();
    defaultChannelMsgResponse.setOk(true);
    Mockito.when(mockSlackClient.chatPostMessage(eq(dmMsgRequest)))
        .thenReturn(defaultChannelMsgResponse);

    // Init with a mock slack client.
    SlackNotificationSink sink = new SlackNotificationSink(mockSlackClient);
    sink.botToken = TEST_BOT_TOKEN;
    sink.init(
        new NotificationSinkConfig(
            ImmutableMap.of("botToken", TEST_BOT_TOKEN, "defaultChannel", "#test"),
            mockEntityClient,
            mockSettingsProvider,
            mockIdentityProvider,
            mockEntityNameProvider,
            mockSecretProvider,
            mockConnectionService,
            mockIntegrationsService,
            TEST_BASE_URL));

    // Now, construct an assertion message request and verify that it is sent to the slack client.
    NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(
        new NotificationMessage()
            .setTemplate(
                com.linkedin.event.notification.template.NotificationTemplateType.valueOf(
                    NotificationTemplateType.BROADCAST_ENTITY_CHANGE.name()))
            .setParameters(
                new StringMap(
                    ImmutableMap.of(
                        "actorUrn",
                        TEST_USER_URN.toString(),
                        "entityType",
                        "Dataset",
                        "entityName",
                        "SampleName",
                        "entityPath",
                        "/dataset/test",
                        "operation",
                        "marked as deprecated",
                        "modifierType",
                        "deprecation"))));

    notificationRequest.setRecipients(
        new NotificationRecipientArray(
            ImmutableList.of(
                // Custom DM recipient
                new NotificationRecipient()
                    .setType(NotificationRecipientType.SLACK_DM)
                    .setId("12345"))));

    // Send the request
    sink.send(opContext, notificationRequest, new NotificationContext());

    Mockito.verify(mockSlackClient, Mockito.times(1)).chatPostMessage(eq(dmMsgRequest));
  }

  @Test
  public void testExternalAssertionNotification() throws Exception {
    /*
     * This test verifies that sending a notification for assertion changes
     * makes requests that are expected.
     */
    SettingsProvider mockSettingsProvider = mock(SettingsProvider.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings(opContext)).thenReturn(enabledSettings());
    ConnectionService mockConnectionService = mock(ConnectionService.class);
    Mockito.when(
            mockConnectionService.getConnectionDetails(any(OperationContext.class), any(Urn.class)))
        .thenReturn(null);
    IdentityProvider mockIdentityProvider = mock(IdentityProvider.class);
    Mockito.when(mockIdentityProvider.getUser(any(OperationContext.class), eq(TEST_USER_URN)))
        .thenReturn(testUser());
    EntityNameProvider mockEntityNameProvider = Mockito.mock(EntityNameProvider.class);
    SecretProvider mockSecretProvider = mock(SecretProvider.class);
    IntegrationsService mockIntegrationsService = mock(IntegrationsService.class);

    // Init the slack client mock
    MethodsClient mockSlackClient = mock(MethodsClient.class);
    UsersLookupByEmailRequest lookupRequests =
        UsersLookupByEmailRequest.builder().email("test@gmail.com").build();
    UsersLookupByEmailResponse lookupResponse = new UsersLookupByEmailResponse();
    lookupResponse.setOk(true);
    User slackUser = new User();
    slackUser.setId("test-id");
    lookupResponse.setUser(slackUser);
    Mockito.when(mockSlackClient.usersLookupByEmail(eq(lookupRequests))).thenReturn(lookupResponse);

    ChatPostMessageRequest dmMsgRequest =
        ChatPostMessageRequest.builder()
            .channel("12345")
            .text(
                String.format(
                    ">%s  *External Assertion* `urn:li:assertion:test` has *passed* for *<%s|%s>*! <%s|View results in dbt>",
                    ":white_check_mark:",
                    "http://localhost:9002/datasets/test",
                    "SampleName",
                    "http://localhost:8084/dbt/results"))
            .iconUrl(String.format("http://localhost:9002%s", ACRYL_LOGO_FILE_PATH))
            .build();
    ChatPostMessageResponse defaultChannelMsgResponse = new ChatPostMessageResponse();
    defaultChannelMsgResponse.setOk(true);
    Mockito.when(mockSlackClient.chatPostMessage(eq(dmMsgRequest)))
        .thenReturn(defaultChannelMsgResponse);

    // Init with a mock slack client.
    SlackNotificationSink sink = new SlackNotificationSink(mockSlackClient);
    sink.botToken = TEST_BOT_TOKEN;
    sink.init(
        new NotificationSinkConfig(
            ImmutableMap.of("botToken", TEST_BOT_TOKEN, "defaultChannel", "#test"),
            mockEntityClient,
            mockSettingsProvider,
            mockIdentityProvider,
            mockEntityNameProvider,
            mockSecretProvider,
            mockConnectionService,
            mockIntegrationsService,
            TEST_BASE_URL));

    // Now, construct an assertion message request and verify that it is sent to the slack client.
    NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(
        new NotificationMessage()
            .setTemplate(
                com.linkedin.event.notification.template.NotificationTemplateType.valueOf(
                    NotificationTemplateType.BROADCAST_ASSERTION_STATUS_CHANGE.name()))
            .setParameters(
                new StringMap(
                    ImmutableMap.of(
                        "assertionUrn",
                        "urn:li:assertion:test",
                        "assertionType",
                        "DATASET",
                        "entityName",
                        "SampleName",
                        "entityPath",
                        "/datasets/test",
                        "result",
                        "SUCCESS",
                        "description",
                        "urn:li:assertion:test",
                        "sourceType",
                        "EXTERNAL",
                        "externalPlatform",
                        "dbt",
                        "externalUrl",
                        "http://localhost:8084/dbt/results"))));

    notificationRequest.setRecipients(
        new NotificationRecipientArray(
            ImmutableList.of(
                // Custom DM recipient
                new NotificationRecipient()
                    .setType(NotificationRecipientType.SLACK_DM)
                    .setId("12345"))));

    // Send the request
    sink.send(opContext, notificationRequest, new NotificationContext());

    Mockito.verify(mockSlackClient, Mockito.times(1)).chatPostMessage(eq(dmMsgRequest));
  }

  @Test
  public void testExternalAssertionNotificationWithPostNotificationExecutionRequest()
      throws Exception {
    /*
     * This test verifies that sending a notification for assertion changes makes requests
     * that are expected, and correctly triggers the right post notification actions.
     */
    SettingsProvider mockSettingsProvider = mock(SettingsProvider.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings(opContext)).thenReturn(enabledSettings());
    ConnectionService mockConnectionService = mock(ConnectionService.class);
    Mockito.when(
            mockConnectionService.getConnectionDetails(any(OperationContext.class), any(Urn.class)))
        .thenReturn(null);
    IdentityProvider mockIdentityProvider = mock(IdentityProvider.class);
    Mockito.when(mockIdentityProvider.getUser(any(OperationContext.class), eq(TEST_USER_URN)))
        .thenReturn(testUser());
    EntityNameProvider mockEntityNameProvider = Mockito.mock(EntityNameProvider.class);
    SecretProvider mockSecretProvider = mock(SecretProvider.class);
    IntegrationsService mockIntegrationsService = mock(IntegrationsService.class);

    // Init the slack client mock
    MethodsClient mockSlackClient = mock(MethodsClient.class);
    UsersLookupByEmailRequest lookupRequests =
        UsersLookupByEmailRequest.builder().email("test@gmail.com").build();
    UsersLookupByEmailResponse lookupResponse = new UsersLookupByEmailResponse();
    lookupResponse.setOk(true);
    User slackUser = new User();
    slackUser.setId("test-id");
    lookupResponse.setUser(slackUser);
    Mockito.when(mockSlackClient.usersLookupByEmail(eq(lookupRequests))).thenReturn(lookupResponse);

    ChatPostMessageRequest dmMsgRequest =
        ChatPostMessageRequest.builder()
            .channel("12345")
            .text(
                String.format(
                    ">%s  *External Assertion* `urn:li:assertion:test` has *passed* for *<%s|%s>*! <%s|View results in dbt>",
                    ":white_check_mark:",
                    "http://localhost:9002/datasets/test",
                    "SampleName",
                    "http://localhost:8084/dbt/results"))
            .iconUrl(String.format("http://localhost:9002%s", ACRYL_LOGO_FILE_PATH))
            .build();
    ChatPostMessageResponse defaultChannelMsgResponse = new ChatPostMessageResponse();
    defaultChannelMsgResponse.setOk(true);
    Mockito.when(mockSlackClient.chatPostMessage(eq(dmMsgRequest)))
        .thenReturn(defaultChannelMsgResponse);

    // Init with a mock slack client.
    SlackNotificationSink sink = new SlackNotificationSink(mockSlackClient);
    sink.botToken = TEST_BOT_TOKEN;
    sink.init(
        new NotificationSinkConfig(
            ImmutableMap.of("botToken", TEST_BOT_TOKEN, "defaultChannel", "#test"),
            mockEntityClient,
            mockSettingsProvider,
            mockIdentityProvider,
            mockEntityNameProvider,
            mockSecretProvider,
            mockConnectionService,
            mockIntegrationsService,
            TEST_BASE_URL));

    // Now, construct an assertion message request and verify that it is sent to the slack client.
    NotificationRequest notificationRequest = new NotificationRequest();
    Map<String, String> paramsMap =
        new HashMap<>(
            Map.of(
                "assertionUrn",
                "urn:li:assertion:test",
                "assertionType",
                "DATASET",
                "entityName",
                "SampleName",
                "entityPath",
                "/datasets/test",
                "result",
                "SUCCESS",
                "description",
                "urn:li:assertion:test",
                "sourceType",
                "EXTERNAL",
                "externalPlatform",
                "dbt",
                "externalUrl",
                "http://localhost:8084/dbt/results",
                Constants.NOTIFICATION_CONNECTION_TEST_EXECUTION_REQUEST_URN_PARAM_KEY,
                TEST_EXECUTION_REQUEST_URN));
    paramsMap.put("requestName", Constants.NOTIFICATION_CONNECTION_TEST_REQUEST_TEMPLATE_NAME);
    notificationRequest.setMessage(
        new NotificationMessage()
            .setTemplate(
                com.linkedin.event.notification.template.NotificationTemplateType.valueOf(
                    NotificationTemplateType.BROADCAST_ASSERTION_STATUS_CHANGE.name()))
            .setParameters(new StringMap(paramsMap)));

    notificationRequest.setRecipients(
        new NotificationRecipientArray(
            ImmutableList.of(
                // Custom DM recipient
                new NotificationRecipient()
                    .setType(NotificationRecipientType.SLACK_DM)
                    .setId("12345"))));

    // Send the request
    sink.send(opContext, notificationRequest, new NotificationContext());

    Mockito.verify(mockSlackClient, Mockito.times(1)).chatPostMessage(eq(dmMsgRequest));

    Mockito.verify(mockEntityClient, Mockito.times(1))
        .ingestProposal(
            eq(opContext),
            eq(
                new MetadataChangeProposal()
                    .setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN))
                    .setEntityType(EXECUTION_REQUEST_ENTITY_NAME)
                    .setAspectName(EXECUTION_REQUEST_RESULT_ASPECT_NAME)
                    .setAspect(
                        GenericRecordUtils.serializeAspect(
                            new ExecutionRequestResult()
                                .setStatus("SUCCESS")
                                .setStructuredReport(
                                    new StructuredExecutionReport()
                                        .setContentType("application/json")
                                        .setType(NOTIFICATION_CONNECTION_TEST_EXECUTION_REPORT_TYPE)
                                        .setSerializedValue(
                                            JSON.serialize(List.of(Collections.emptyMap()))))))
                    .setChangeType(ChangeType.UPSERT)),
            any(Boolean.class));
  }

  @Test
  public void testAssertionNotificationWithRetry() throws Exception {
    /*
     * This test verifies that sending a notification for assertion changes makes
     * requests that are expected and retries up to 3 extra time on rate limit response
     */
    SettingsProvider mockSettingsProvider = mock(SettingsProvider.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings(opContext)).thenReturn(enabledSettings());
    ConnectionService mockConnectionService = mock(ConnectionService.class);
    Mockito.when(
            mockConnectionService.getConnectionDetails(any(OperationContext.class), any(Urn.class)))
        .thenReturn(null);
    IdentityProvider mockIdentityProvider = mock(IdentityProvider.class);
    Mockito.when(mockIdentityProvider.getUser(any(OperationContext.class), eq(TEST_USER_URN)))
        .thenReturn(testUser());
    EntityNameProvider mockEntityNameProvider = Mockito.mock(EntityNameProvider.class);
    SecretProvider mockSecretProvider = mock(SecretProvider.class);
    IntegrationsService mockIntegrationsService = mock(IntegrationsService.class);

    // Init the slack client mock
    MethodsClient mockSlackClient = mock(MethodsClient.class);
    UsersLookupByEmailRequest lookupRequests =
        UsersLookupByEmailRequest.builder().email("test@gmail.com").build();
    UsersLookupByEmailResponse lookupResponse = new UsersLookupByEmailResponse();
    lookupResponse.setOk(true);
    User slackUser = new User();
    slackUser.setId("test-id");
    lookupResponse.setUser(slackUser);
    Mockito.when(mockSlackClient.usersLookupByEmail(eq(lookupRequests))).thenReturn(lookupResponse);

    ChatPostMessageRequest dmMsgRequest =
        ChatPostMessageRequest.builder()
            .channel("12345")
            .text(
                String.format(
                    ">%s  *Column Assertion* `column x is greater than y` has *passed* for *<%s|%s>*! <%s|View results>",
                    ":white_check_mark:",
                    "http://localhost:9002/datasets/test",
                    "SampleName",
                    "http://localhost:9002/datasets/test/Validation/Assertions?assertion_urn=urn%3Ali%3Aassertion%3Atest"))
            .iconUrl(String.format("http://localhost:9002%s", ACRYL_LOGO_FILE_PATH))
            .build();

    Response response = mock(Response.class);
    Mockito.when(response.code()).thenReturn(429);
    Mockito.when(response.header("retry-after")).thenReturn("1");
    SlackApiException e = new SlackApiException(response, "");
    Mockito.when(mockSlackClient.chatPostMessage(eq(dmMsgRequest))).thenThrow(e);

    // Init with a mock slack client.
    SlackNotificationSink sink = new SlackNotificationSink(mockSlackClient);
    sink.botToken = TEST_BOT_TOKEN;
    sink.init(
        new NotificationSinkConfig(
            ImmutableMap.of("botToken", TEST_BOT_TOKEN, "defaultChannel", "#test"),
            mockEntityClient,
            mockSettingsProvider,
            mockIdentityProvider,
            mockEntityNameProvider,
            mockSecretProvider,
            mockConnectionService,
            mockIntegrationsService,
            TEST_BASE_URL));

    // Now, construct an assertion message request and verify that it is sent to the slack client.
    NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(
        new NotificationMessage()
            .setTemplate(
                com.linkedin.event.notification.template.NotificationTemplateType.valueOf(
                    NotificationTemplateType.BROADCAST_ASSERTION_STATUS_CHANGE.name()))
            .setParameters(
                new StringMap(
                    ImmutableMap.of(
                        "assertionUrn",
                        "urn:li:assertion:test",
                        "assertionType",
                        "FIELD",
                        "entityName",
                        "SampleName",
                        "entityPath",
                        "/datasets/test",
                        "result",
                        "SUCCESS",
                        "description",
                        "column x is greater than y",
                        "sourceType",
                        "NATIVE"))));

    notificationRequest.setRecipients(
        new NotificationRecipientArray(
            ImmutableList.of(
                // Custom DM recipient
                new NotificationRecipient()
                    .setType(NotificationRecipientType.SLACK_DM)
                    .setId("12345"))));

    // Send the request
    sink.send(opContext, notificationRequest, new NotificationContext());

    // should retry a maximum number of 2 time so 3 times total even if we keep hitting the rate
    // limit
    Mockito.verify(mockSlackClient, Mockito.times(3)).chatPostMessage(eq(dmMsgRequest));
  }

  @Test
  public void testSlackSinkEnabledV2() throws Exception {
    /*
     * This test verifies that the sink does not include template types supported by
     * the V2 (Python Integrations Service) sink when the V2 sink is enabled.
     */
    SettingsProvider mockSettingsProvider = Mockito.mock(SettingsProvider.class);
    ConnectionService mockConnectionService = Mockito.mock(ConnectionService.class);
    IdentityProvider mockIdentityProvider = Mockito.mock(IdentityProvider.class);
    EntityNameProvider mockEntityNameProvider = Mockito.mock(EntityNameProvider.class);
    SecretProvider mockSecretProvider = Mockito.mock(SecretProvider.class);
    IntegrationsService mockIntegrationsService = Mockito.mock(IntegrationsService.class);
    MethodsClient mockClient = Mockito.mock(MethodsClient.class);

    // Init with a mock slack client.
    SlackNotificationSink sink = new SlackNotificationSink(mockClient);
    sink.botToken = TEST_BOT_TOKEN;
    sink.init(
        new NotificationSinkConfig(
            ImmutableMap.of(
                "botToken", TEST_BOT_TOKEN,
                "defaultChannel", "#test",
                "slackSinkV2Enabled", "true"),
            mockEntityClient,
            mockSettingsProvider,
            mockIdentityProvider,
            mockEntityNameProvider,
            mockSecretProvider,
            mockConnectionService,
            mockIntegrationsService,
            TEST_BASE_URL));

    // Ensure that we support all templates except those supported by V2 sink.
    Assert.assertEquals(
        sink.templates(),
        ImmutableList.of(
            NotificationTemplateType.CUSTOM,
            NotificationTemplateType.BROADCAST_NEW_PROPOSAL,
            NotificationTemplateType.BROADCAST_PROPOSAL_STATUS_CHANGE,
            NotificationTemplateType.BROADCAST_ENTITY_CHANGE,
            NotificationTemplateType.BROADCAST_INGESTION_RUN_CHANGE,
            NotificationTemplateType.BROADCAST_ASSERTION_STATUS_CHANGE));
  }

  // =========================================================================
  // TESTS: sendBroadcastProposalStatusChangeNotifications(...)
  // =========================================================================

  @Test
  public void testSendBroadcastProposalStatusChangeNotificationsCreatorPresentCorrectOrigin() {
    // Setup a NotificationRequest that has a "creatorUrn" which matches one of the recipients,
    // and that recipient's origin is ACTOR_NOTIFICATION, triggering the special path for the
    // recipient.
    SlackNotificationSink localSpySink = Mockito.spy(sink);

    final String creatorUrnStr = "urn:li:corpuser:creator123";
    Urn creatorUrn = UrnUtils.getUrn(creatorUrnStr);

    NotificationRecipient creatorRecipient =
        new NotificationRecipient()
            .setActor(creatorUrn)
            .setType(NotificationRecipientType.SLACK_DM)
            .setId("test-1")
            .setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    NotificationRecipient otherRecipient =
        new NotificationRecipient()
            .setActor(UrnUtils.getUrn("urn:li:corpuser:someone_else"))
            .setType(NotificationRecipientType.SLACK_DM)
            .setId("test-2")
            .setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    List<NotificationRecipient> recipientList = Arrays.asList(creatorRecipient, otherRecipient);

    NotificationMessage msg = new NotificationMessage();
    Map<String, String> params = new HashMap<>();
    params.put("creatorUrn", creatorUrnStr);
    params.put("actorName", "Reviewer Person");
    params.put("action", "accepted");
    params.put("entityName", "TestEntity");
    params.put("operation", "add");
    params.put("modifierType", "Tag(s)");
    msg.setParameters(new StringMap(params));

    NotificationRequest request =
        new NotificationRequest()
            .setRecipients(new NotificationRecipientArray(recipientList))
            .setMessage(msg);

    // Mock the downstream calls
    ChatPostMessageResponse mockBroadcastResponse = mock(ChatPostMessageResponse.class);
    doReturn(Collections.singleton(mockBroadcastResponse))
        .when(localSpySink)
        .sendBroadcastNotification(
            eq(opContext), eq(Collections.singletonList(otherRecipient)), anyString(), any());
    ChatPostMessageResponse mockPersonalResponse = mock(ChatPostMessageResponse.class);
    doReturn(mockPersonalResponse)
        .when(localSpySink)
        .sendNotificationToRecipient(eq(opContext), eq(creatorRecipient), anyString(), any());

    // Execute
    Set<ChatPostMessageResponse> responses =
        localSpySink.sendBroadcastProposalStatusChangeNotifications(opContext, request);

    // Verify the results
    // 1) We expect exactly 2 responses: one personal, one broadcast
    assertEquals(responses.size(), 2);

    // 2) We verify that buildProposerProposalStatusChangeMessage was used for the creator
    verify(localSpySink, times(1))
        .buildProposerProposalStatusChangeMessage(Mockito.eq(request));

    // 3) We verify that buildProposalStatusChangeMessage was used for the others
    verify(localSpySink, times(1))
        .buildProposalStatusChangeMessage(Mockito.eq(request));

    // 4) We verify the calls to sendNotificationToRecipient (personal) and
    // sendBroadcastNotification (others)
    verify(localSpySink, times(1))
        .sendNotificationToRecipient(
            Mockito.eq(opContext), Mockito.eq(creatorRecipient), anyString(), any());
    verify(localSpySink, times(1))
        .sendBroadcastNotification(
            Mockito.eq(opContext),
            Mockito.eq(Collections.singletonList(otherRecipient)),
            anyString(),
            any());
  }

  @Test
  public void testSendBroadcastProposalStatusChangeNotificationsCreatorPresentWrongOrigin() {
    // The "creatorUrn" matches a recipient, but the origin is not ACTOR_NOTIFICATION,
    // so the personal message path should NOT be triggered.

    SlackNotificationSink localSpySink = Mockito.spy(sink);

    final String creatorUrnStr = "urn:li:corpuser:creatorXYZ";
    Urn creatorUrn = UrnUtils.getUrn(creatorUrnStr);

    NotificationRecipient creatorRecipient =
        new NotificationRecipient()
            .setActor(creatorUrn)
            .setType(NotificationRecipientType.SLACK_DM)
            .setId("test")
            // Not ACTOR_NOTIFICATION => we skip personal message
            .setOrigin(NotificationRecipientOriginType.SUBSCRIPTION);

    NotificationMessage msg = new NotificationMessage();
    Map<String, String> params = new HashMap<>();
    params.put("creatorUrn", creatorUrnStr);
    params.put("actorName", "Another Person");
    params.put("action", "rejected");
    params.put("entityName", "DatasetNoPersonalPath");
    params.put("operation", "create");
    params.put("modifierType", "Glossary Term");
    msg.setParameters(new StringMap(params));

    NotificationRequest request =
        new NotificationRequest()
            .setRecipients(
                new NotificationRecipientArray(Collections.singletonList(creatorRecipient)))
            .setMessage(msg);

    // Mock
    ChatPostMessageResponse broadcastResponse = mock(ChatPostMessageResponse.class);
    doReturn(Collections.singleton(broadcastResponse))
        .when(localSpySink)
        .sendBroadcastNotification(any(), anyList(), anyString(), any());

    // Execute
    Set<ChatPostMessageResponse> responses =
        localSpySink.sendBroadcastProposalStatusChangeNotifications(opContext, request);

    // Verify
    // 1) Only one broadcast call, no personal path
    assertEquals(responses.size(), 1);

    // 2) We confirm buildProposerProposalStatusChangeMessage was NEVER used
    verify(localSpySink, never()).buildProposerProposalStatusChangeMessage(any());

    // 3) We confirm we did build the normal message
    verify(localSpySink, times(1))
        .buildProposalStatusChangeMessage(Mockito.eq(request));
  }

  @Test
  public void testSendBroadcastProposalStatusChangeNotificationsCreatorAbsent() {
    // The "creatorUrn" param is present but no matching recipient => skip personal path
    SlackNotificationSink localSpySink = Mockito.spy(sink);

    final String creatorUrnStr = "urn:li:corpuser:creatorMissing";
    NotificationRecipient randomRecipient =
        new NotificationRecipient()
            .setActor(UrnUtils.getUrn("urn:li:corpuser:someoneElse"))
            .setType(NotificationRecipientType.SLACK_DM)
            .setId("test")
            .setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION);

    NotificationMessage msg = new NotificationMessage();
    Map<String, String> params = new HashMap<>();
    params.put("creatorUrn", creatorUrnStr);
    params.put("actorName", "Alice");
    params.put("action", "accepted");
    params.put("entityName", "EntityWithoutCreator");
    params.put("operation", "add");
    params.put("modifierType", "Tag(s)");
    msg.setParameters(new StringMap(params));

    NotificationRequest request =
        new NotificationRequest()
            .setRecipients(
                new NotificationRecipientArray(Collections.singletonList(randomRecipient)))
            .setMessage(msg);

    // Mock
    ChatPostMessageResponse broadcastResponse = mock(ChatPostMessageResponse.class);
    doReturn(Collections.singleton(broadcastResponse))
        .when(localSpySink)
        .sendBroadcastNotification(any(), anyList(), anyString(), any());

    // Execute
    Set<ChatPostMessageResponse> responses =
        localSpySink.sendBroadcastProposalStatusChangeNotifications(opContext, request);

    // Verify
    assertEquals(responses.size(), 1);
    verify(localSpySink, never()).buildProposerProposalStatusChangeMessage(any());
    verify(localSpySink, times(1))
        .buildProposalStatusChangeMessage(Mockito.eq(request));
  }

  // =========================================================================
  // TESTS: buildNewProposalMessage(...)
  // =========================================================================
  @Test
  public void testBuildNewProposalTagsWithSubResource() {
    // Example: "John Joyce has proposed Tag(s) [PII, Sensitive] for schema field foo of
    // *SampleKafkaDataset*. <link>"
    Map<String, String> params = new HashMap<>();
    params.put("actorName", "John Joyce");
    params.put("modifierType", "Tag(s)");
    params.put("modifierNames", "[\"PII\",\"Sensitive\"]");
    params.put("modifierPaths", "[\"/tag/PII\",\"/tag/Sensitive\"]");
    params.put("subResourceType", "schemaField");
    params.put("subResource", "foo");
    params.put("entityName", "SampleKafkaDataset");
    params.put("operation", "add");

    NotificationMessage msg = new NotificationMessage();
    msg.setParameters(new StringMap(params));
    NotificationRequest request = new NotificationRequest().setMessage(msg);

    String messageText = sink.buildNewProposalMessage(request);

    assertTrue(messageText.contains("John Joyce"), "Actor name should appear");
    assertTrue(messageText.contains("Tag(s)"), "Modifier type should appear");
    assertTrue(
        messageText.contains("PII") && messageText.contains("Sensitive"),
        "Modifier names should appear");
    assertTrue(
        messageText.contains("foo") && messageText.contains("schema field"),
        "Subresource references should appear");
    assertTrue(messageText.contains("SampleKafkaDataset"), "Entity name should appear");
    assertTrue(messageText.contains("View details"), "Link to details should appear");
  }

  @Test
  public void testBuildNewProposalGlossaryTermsWithoutSubResource() {
    // Example: "John Joyce has proposed Tag(s) [PII, Sensitive] for schema field foo of
    // *SampleKafkaDataset*. <link>"
    Map<String, String> params = new HashMap<>();
    params.put("actorName", "John Joyce");
    params.put("modifierType", "Glossary Term(s)");
    params.put("modifierNames", "[\"PII\",\"Sensitive\"]");
    params.put("modifierPaths", "[\"/glossaryTerm/PII\",\"/glossaryTerm/Sensitive\"]");
    params.put("entityName", "SampleKafkaDataset");
    params.put("operation", "add");

    NotificationMessage msg = new NotificationMessage();
    msg.setParameters(new StringMap(params));
    NotificationRequest request = new NotificationRequest().setMessage(msg);

    String messageText = sink.buildNewProposalMessage(request);

    assertTrue(messageText.contains("John Joyce"), "Actor name should appear");
    assertTrue(messageText.contains("Glossary Term(s)"), "Modifier type should appear");
    assertTrue(
        messageText.contains("PII") && messageText.contains("Sensitive"),
        "Modifier names should appear");
    assertTrue(messageText.contains("SampleKafkaDataset"), "Entity name should appear");
    assertTrue(messageText.contains("View details"), "Link to details should appear");
  }

  @Test
  public void
      testBuildNewProposalMessageWithoutSubResourceAndCreatingGlossaryTermWithParentGroup() {
    // "John Joyce has proposed creating Glossary Term named Email Address in Term Group *PII*."
    Map<String, String> params = new HashMap<>();
    params.put("actorName", "John Joyce");
    params.put("modifierType", "Glossary Term");
    params.put("entityName", "Email Address"); // we interpret this as the new term name
    params.put("operation", "create");
    params.put("parentTermGroupName", "PII"); // indicates a parent group
    NotificationMessage msg = new NotificationMessage();
    msg.setParameters(new StringMap(params));

    NotificationRequest request = new NotificationRequest().setMessage(msg);

    String messageText = sink.buildNewProposalMessage(request);

    assertTrue(messageText.contains("John Joyce"), "Should include actor name");
    assertTrue(
        messageText.contains("creating Glossary Term named Email Address"),
        "Should mention creating the glossary term with that name");
    assertTrue(
        messageText.contains("in Term Group *PII*"), "Should mention the parent glossary group");
    assertTrue(messageText.contains("View details"), "Should mention the details link");
  }

  @Test
  public void
      testBuildNewProposalMessageWithoutSubResourceAndCreatingGlossaryTermGroupWithParentGroup() {
    // "John Joyce has proposed creating Glossary Term named Email Address in Term Group *PII*."
    Map<String, String> params = new HashMap<>();
    params.put("actorName", "John Joyce");
    params.put("modifierType", "Glossary Term Group");
    params.put("entityName", "PII"); // we interpret this as the new term name
    params.put("operation", "create");
    params.put("parentTermGroupName", "Root"); // indicates a parent group
    NotificationMessage msg = new NotificationMessage();
    msg.setParameters(new StringMap(params));

    NotificationRequest request = new NotificationRequest().setMessage(msg);

    String messageText = sink.buildNewProposalMessage(request);

    assertTrue(messageText.contains("John Joyce"), "Should include actor name");
    assertTrue(
        messageText.contains("creating Glossary Term Group named PII"),
        "Should mention creating the glossary term with that name");
    assertTrue(
        messageText.contains("in Term Group *Root*"), "Should mention the parent glossary group");
    assertTrue(messageText.contains("View details"), "Should mention the details link");
  }

  @Test
  public void testBuildNewProposalMessageRegularCaseNoSubResource() {
    // "John Joyce has proposed to add Tag(s) 'PII' for *SampleKafkaDataset*."
    Map<String, String> params = new HashMap<>();
    params.put("actorName", "John Joyce");
    params.put("modifierType", "Tag(s)");
    params.put("modifierNames", "[\"PII\"]");
    params.put("modifierPaths", "[\"/tag/PII\"]");
    params.put("entityName", "SampleKafkaDataset");
    params.put("operation", "add");
    NotificationMessage msg = new NotificationMessage();
    msg.setParameters(new StringMap(params));

    NotificationRequest request = new NotificationRequest().setMessage(msg);

    String messageText = sink.buildNewProposalMessage(request);

    assertTrue(
        messageText.contains("proposed to add Tag(s) <http://localhost:9002/tag/PII|PII>"),
        "Should mention link to PII");
    assertTrue(messageText.contains("SampleKafkaDataset"), "Should mention the entity name");
    assertTrue(messageText.contains("View details"), "Should include the details link");
  }

  @Test
  public void testBuildNewProposalMessageStructuredProperties() {

    // Configure mock sink.

    Map<String, String> params = new HashMap<>();
    params.put("actorName", "John Joyce");
    params.put("operation", "add");
    params.put("modifierType", "Structured Properties");
    params.put("modifierNames", "[\"Test Property\", \"Test Property 2\"]");
    params.put("entityName", "TestEntity");

    NotificationMessage msg = new NotificationMessage();
    msg.setParameters(new StringMap(params));
    NotificationRequest request = new NotificationRequest().setMessage(msg);

    String messageText = sink.buildNewProposalMessage(request);

    assertTrue(
        messageText.contains("proposed to add Structured Properties"),
        "Should mention link to structured properties");
    assertTrue(messageText.contains("Test Property"), "Should mention the first property");
    assertTrue(messageText.contains("Test Property 2"), "Should mention the first property 2");
    assertTrue(messageText.contains("TestEntity"), "Should mention the entity name");
    assertTrue(messageText.contains("View details"), "Should include the details link");
  }

  // =========================================================================
  // TESTS: buildProposalStatusChangeMessage(...)
  // =========================================================================
  @Test
  public void testBuildProposalStatusChangeMessageWithSubResourceAccepted() {

    // Example: "*John Joyce* has accepted the proposal to add Tag(s) <...> for *bar* of
    // *SampleKafkaDataset*."
    Map<String, String> params = new HashMap<>();
    params.put("actorName", "John Joyce");
    params.put("action", "accepted");
    params.put("operation", "add");
    params.put("modifierType", "Tag(s)");
    params.put("modifierNames", "[\"Security\",\"Confidential\"]");
    params.put("modifierPaths", "[\"/tag/Security\",\"/tag/Confidential\"]");
    params.put("subResourceType", "schemaField");
    params.put("subResource", "bar");
    params.put("entityName", "SampleKafkaDataset");

    NotificationMessage msg = new NotificationMessage();
    msg.setParameters(new StringMap(params));
    NotificationRequest request = new NotificationRequest().setMessage(msg);

    String msgText = sink.buildProposalStatusChangeMessage(request);
    assertTrue(msgText.contains("*John Joyce* has accepted the proposal to add Tag(s)"));
    assertTrue(msgText.contains("Security") && msgText.contains("Confidential"));
    assertTrue(msgText.contains("bar"));
    assertTrue(msgText.contains("SampleKafkaDataset"));
    assertTrue(msgText.contains("View details"));
  }

  @Test
  public void testBuildProposalStatusChangeMessageWithoutSubResourceRejected() {

    // "*Alice* has rejected the proposal to create Glossary Term 'Payment Days' for
    // *FinancialDataset*."
    Map<String, String> params = new HashMap<>();
    params.put("actorName", "Alice");
    params.put("action", "rejected");
    params.put("operation", "add");
    params.put("modifierType", "Glossary Term(s)");
    params.put("modifierNames", "[\"Payment Days\"]");
    params.put("modifierPaths", "[]"); // no linking
    params.put("entityName", "FinancialDataset");

    NotificationMessage msg = new NotificationMessage();
    msg.setParameters(new StringMap(params));
    NotificationRequest request = new NotificationRequest().setMessage(msg);

    String msgText = sink.buildProposalStatusChangeMessage(request);
    assertTrue(
        msgText.contains(
            "*Alice* has rejected the proposal to add Glossary Term(s) 'Payment Days'"));
    assertTrue(msgText.contains("FinancialDataset"));
    assertTrue(msgText.contains("View details"));
  }

  @Test
  public void testBuildProposalStatusChangeMessageStructuredProperties() {

    Map<String, String> params = new HashMap<>();
    params.put("actorName", "Approver");
    params.put("action", "accepted");
    params.put("operation", "add");
    params.put("modifierType", "Structured Properties");
    params.put("modifierNames", "[\"Test Property\", \"Test Property 2\"]");
    params.put("entityName", "TestEntity");

    NotificationMessage msg = new NotificationMessage();
    msg.setParameters(new StringMap(params));
    NotificationRequest request = new NotificationRequest().setMessage(msg);

    String messageText = sink.buildProposalStatusChangeMessage(request);

    assertTrue(
        messageText.contains(
            "*Approver* has accepted the proposal to add Structured Properties 'Test Property' and 'Test Property 2' for *TestEntity*. <http://localhost:9002/requests|View details>"));
  }

  // =========================================================================
  // TESTS: buildProposerProposalStatusChangeMessage(...)
  // =========================================================================
  @Test
  public void testBuildProposerProposalStatusChangeMessageWithSubResource() {
    // "Your proposal to add Tag(s) <...> for *bar* of *SampleKafkaDataset* has been *accepted*."
    Map<String, String> params = new HashMap<>();
    params.put("action", "accepted");
    params.put("operation", "add");
    params.put("modifierType", "Tag(s)");
    params.put("modifierNames", "[\"Internal\",\"Critical\"]");
    params.put("modifierPaths", "[\"/tag/Internal\",\"/tag/Critical\"]");
    params.put("subResourceType", "schemaField");
    params.put("subResource", "bar");
    params.put("entityName", "SampleKafkaDataset");

    NotificationMessage msg = new NotificationMessage();
    msg.setParameters(new StringMap(params));
    NotificationRequest request = new NotificationRequest().setMessage(msg);

    String msgText = sink.buildProposerProposalStatusChangeMessage(request);
    assertTrue(
        msgText.contains(
            "Your proposal to add Tag(s) <http://localhost:9002/tag/Internal|Internal>"));
    assertTrue(msgText.contains("bar"));
    assertTrue(msgText.contains("SampleKafkaDataset"));
    assertTrue(msgText.contains("has been *accepted*"));
  }

  @Test
  public void testBuildProposerProposalStatusChangeMessageWithoutSubResource() {
    // "Your proposal to create Glossary Term <...> for *EntityWithoutResource* has been
    // *rejected*."
    Map<String, String> params = new HashMap<>();
    params.put("action", "rejected");
    params.put("operation", "add");
    params.put("modifierType", "Glossary Term(s)");
    params.put("modifierNames", "[\"PII_Term\"]");
    params.put("modifierPaths", "[\"/term/PII_Term\"]");
    params.put("entityName", "EntityWithoutResource");

    NotificationMessage msg = new NotificationMessage();
    msg.setParameters(new StringMap(params));
    NotificationRequest request = new NotificationRequest().setMessage(msg);

    String msgText = sink.buildProposerProposalStatusChangeMessage(request);
    assertTrue(
        msgText.contains(
            "Your proposal to add Glossary Term(s) <http://localhost:9002/term/PII_Term|PII_Term>"));
    assertTrue(msgText.contains("for *EntityWithoutResource* has been *rejected*"));
  }

  private IdentityProvider.User testUser() {
    IdentityProvider.User testUser = new IdentityProvider.User();
    testUser.setSlack("test");
    testUser.setActive(true);
    testUser.setEmail("test@gmail.com");
    testUser.setFirstName("Test");
    testUser.setLastName("User");
    return testUser;
  }

  private GlobalSettingsInfo disabledSettings() {
    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();
    globalSettingsInfo.setNotifications(new GlobalNotificationSettings());

    // disabled slack settings..
    GlobalIntegrationSettings globalIntegrationSettings = new GlobalIntegrationSettings();
    globalIntegrationSettings.setSlackSettings(new SlackIntegrationSettings().setEnabled(false));
    globalSettingsInfo.setIntegrations(globalIntegrationSettings);
    return globalSettingsInfo;
  }

  private GlobalSettingsInfo enabledSettings() {
    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();
    globalSettingsInfo.setNotifications(new GlobalNotificationSettings());

    // Enabled slack settings..
    GlobalIntegrationSettings globalIntegrationSettings = new GlobalIntegrationSettings();
    globalIntegrationSettings.setSlackSettings(
        new SlackIntegrationSettings().setEnabled(true).setDefaultChannelName("#test"));
    globalSettingsInfo.setIntegrations(globalIntegrationSettings);
    return globalSettingsInfo;
  }

  private GlobalSettingsInfo emptySettings() {
    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();
    globalSettingsInfo.setNotifications(new GlobalNotificationSettings());

    // Enabled slack settings..
    GlobalIntegrationSettings globalIntegrationSettings = new GlobalIntegrationSettings();
    globalIntegrationSettings.setSlackSettings(new SlackIntegrationSettings()); // Empty
    globalSettingsInfo.setIntegrations(globalIntegrationSettings);
    return globalSettingsInfo;
  }
}
