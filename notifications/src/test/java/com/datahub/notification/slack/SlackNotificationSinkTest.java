package com.datahub.notification.slack;

import static com.linkedin.metadata.AcrylConstants.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

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
import com.linkedin.connection.DataHubConnectionDetails;
import com.linkedin.connection.DataHubConnectionDetailsType;
import com.linkedin.connection.DataHubJsonConnection;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.NotificationMessage;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientArray;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import okhttp3.Response;
import org.mockito.Mockito;
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

  @BeforeTest
  public void setup() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    mockEntityClient = mock(EntityClient.class);
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
    Mockito.when(mockSlack.methods(Mockito.anyString())).thenReturn(mock(MethodsClient.class));

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

    Assert.assertTrue(sink.isEnabled(opContext));
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

    Assert.assertTrue(sink.isEnabled(opContext));
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

    Assert.assertTrue(sink.isEnabled(opContext));
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

    Assert.assertTrue(sink.isEnabled(opContext));
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

    Assert.assertTrue(sink.isEnabled(opContext));
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
