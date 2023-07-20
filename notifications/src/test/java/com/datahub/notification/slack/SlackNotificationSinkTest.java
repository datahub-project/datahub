package com.datahub.notification.slack;

import com.datahub.notification.provider.IdentityProvider;
import com.datahub.notification.NotificationContext;
import com.datahub.notification.NotificationSinkConfig;
import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.provider.SecretProvider;
import com.datahub.notification.provider.SettingsProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.connection.DataHubConnectionDetails;
import com.linkedin.connection.DataHubConnectionDetailsType;
import com.linkedin.connection.DataHubJsonConnection;
import com.linkedin.data.template.StringMap;
import com.linkedin.event.notification.NotificationMessage;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientArray;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.metadata.connection.ConnectionService;
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
import java.util.Collections;
import okhttp3.Response;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.datahub.notification.slack.SlackNotificationSink.*;
import static com.linkedin.metadata.AcrylConstants.*;
import static com.linkedin.metadata.Constants.*;


public class SlackNotificationSinkTest {

  private static final Urn TEST_USER_URN = Urn.createFromTuple(CORP_USER_ENTITY_NAME, "test");
  private static final String TEST_BASE_URL = "http://localhost:9002";

  @Test
  public void testInit() throws Exception {
    // Verify that init doesn't throw when empty configs are provided,
    // and when populated configs are provided.
    SettingsProvider mockSettingsProvider = Mockito.mock(SettingsProvider.class);
    IdentityProvider mockIdentityProvider = Mockito.mock(IdentityProvider.class);
    SecretProvider mockSecretProvider = Mockito.mock(SecretProvider.class);
    SlackNotificationSink sink = new SlackNotificationSink();

    ConnectionService mockConnectionService = Mockito.mock(ConnectionService.class);
    mockConnectionService = Mockito.mock(ConnectionService.class);
    Mockito.when(mockConnectionService.getConnectionDetails(Mockito.any(Urn.class)))
      .thenReturn(
          null
      );

    // Case 1: No Connection, Empty Static Config
    sink.init(new NotificationSinkConfig(
        Collections.emptyMap(),
        mockSettingsProvider,
        mockIdentityProvider,
        mockSecretProvider,
        mockConnectionService,
        TEST_BASE_URL
    ));

    // Case 2: No Connection, Static config with bot token and default channel
    sink.init(new NotificationSinkConfig(
        ImmutableMap.of(
            "botToken",
            "abc",
            "defaultChannel",
            "#test",
            "retryEnabled",
            "true"
        ),
        mockSettingsProvider,
        mockIdentityProvider,
        mockSecretProvider,
        mockConnectionService,
        TEST_BASE_URL
    ));
  }

  @Test
  public void testIsEnabled() {

    // Case 1: Slack configs are stored in new connection object
    SettingsProvider mockSettingsProvider = Mockito.mock(SettingsProvider.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings()).thenReturn(
        enabledSettings()
    );
    ConnectionService mockConnectionService = Mockito.mock(ConnectionService.class);
    Mockito.when(mockConnectionService.getConnectionDetails(Mockito.eq(SLACK_CONNECTION_URN)))
        .thenReturn(
            new DataHubConnectionDetails()
                .setType(DataHubConnectionDetailsType.JSON)
                .setJson(new DataHubJsonConnection()
                    .setEncryptedBlob("blob")
                )
        );
    IdentityProvider mockIdentityProvider = Mockito.mock(IdentityProvider.class);
    Mockito.when(mockIdentityProvider.getUser(TEST_USER_URN)).thenReturn(testUser());
    SecretProvider mockSecretProvider = Mockito.mock(SecretProvider.class);
    Mockito.when(mockSecretProvider.decryptSecret("blob")).thenReturn("{\"bot_token\": \"test-token\"}");

    Slack mockSlack = Mockito.mock(Slack.class);
    Mockito.when(mockSlack.methods(Mockito.anyString())).thenReturn(Mockito.mock(MethodsClient.class));

    SlackNotificationSink sink = new SlackNotificationSink(mockSlack);

    sink.init(new NotificationSinkConfig(
        ImmutableMap.of(
            "botToken",
            "abc",
            "defaultChannel",
            "#test"
        ),
        mockSettingsProvider,
        mockIdentityProvider,
        mockSecretProvider,
        mockConnectionService,
        TEST_BASE_URL
    ));

    Assert.assertTrue(sink.isEnabled());

    // Case 2: Slack configs are stored in legacy settings.
    mockConnectionService = Mockito.mock(ConnectionService.class);
    Mockito.when(mockConnectionService.getConnectionDetails(Mockito.eq(SLACK_CONNECTION_URN)))
        .thenReturn(
            null
        );
    sink.init(new NotificationSinkConfig(
        ImmutableMap.of(
            "botToken",
            "abc",
            "defaultChannel",
            "#test"
        ),
        mockSettingsProvider,
        mockIdentityProvider,
        mockSecretProvider,
        mockConnectionService,
        TEST_BASE_URL
    ));

    Assert.assertTrue(sink.isEnabled());

    // Case 3: Slack configs are stored in static configs
    mockSettingsProvider = Mockito.mock(SettingsProvider.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings())
        .thenReturn(
            emptySettings()
        );
    mockConnectionService = Mockito.mock(ConnectionService.class);
    Mockito.when(mockConnectionService.getConnectionDetails(Mockito.eq(SLACK_CONNECTION_URN)))
        .thenReturn(
            null
        );
    sink.init(new NotificationSinkConfig(
        ImmutableMap.of(
            "botToken",
            "abc",
            "defaultChannel",
            "#test"
        ),
        mockSettingsProvider,
        mockIdentityProvider,
        mockSecretProvider,
        mockConnectionService,
        TEST_BASE_URL
    ));

    Assert.assertTrue(sink.isEnabled());
  }

  @Test
  public void testSendEnabledMultipleRecipientTypes() throws Exception {

    /*
     * This test verifies that sending a notification of type "CUSTOM"
     * works for a specific user, a custom channel, and the default configured
     * channel works as expected in the success case.
     */

    SettingsProvider mockSettingsProvider = Mockito.mock(SettingsProvider.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings()).thenReturn(
        enabledSettings()
    );
    ConnectionService mockConnectionService = Mockito.mock(ConnectionService.class);
    Mockito.when(mockConnectionService.getConnectionDetails(Mockito.any(Urn.class)))
      .thenReturn(
          null
      );
    IdentityProvider mockIdentityProvider = Mockito.mock(IdentityProvider.class);
    Mockito.when(mockIdentityProvider.getUser(TEST_USER_URN)).thenReturn(testUser());
    SecretProvider mockSecretProvider = Mockito.mock(SecretProvider.class);

    // Init the slack client mock
    MethodsClient mockSlackClient = Mockito.mock(MethodsClient.class);
    UsersLookupByEmailRequest lookupRequests = UsersLookupByEmailRequest.builder()
        .email("test@gmail.com")
        .build();
    UsersLookupByEmailResponse lookupResponse = new UsersLookupByEmailResponse();
    lookupResponse.setOk(true);
    User slackUser = new User();
    slackUser.setId("test-id");
    lookupResponse.setUser(slackUser);
    Mockito.when(mockSlackClient.usersLookupByEmail(Mockito.eq(lookupRequests))).thenReturn(lookupResponse);

    ChatPostMessageRequest userMsgRequest = ChatPostMessageRequest.builder()
        .channel("test-id")
        .text("*Test Message Title*\n\nTest Message Body")
        .iconUrl(String.format("http://localhost:9002%s", ACRYL_LOGO_FILE_PATH))
        .build();
    ChatPostMessageResponse usrMsgResponse = new ChatPostMessageResponse();
    usrMsgResponse.setOk(true);
    Mockito.when(mockSlackClient.chatPostMessage(Mockito.eq(userMsgRequest))).thenReturn(usrMsgResponse);

    ChatPostMessageRequest channelMsgRequest = ChatPostMessageRequest.builder()
        .channel("#test-channel")
        .text("*Test Message Title*\n\nTest Message Body")
        .iconUrl(String.format("http://localhost:9002%s", ACRYL_LOGO_FILE_PATH))
        .build();
    ChatPostMessageResponse channelMsgResponse = new ChatPostMessageResponse();
    channelMsgResponse.setOk(true);
    Mockito.when(mockSlackClient.chatPostMessage(Mockito.eq(channelMsgRequest))).thenReturn(channelMsgResponse);

    ChatPostMessageRequest defaultChannelMsgRequest = ChatPostMessageRequest.builder()
        .channel("#test")
        .text("*Test Message Title*\n\nTest Message Body")
        .iconUrl(String.format("http://localhost:9002%s", ACRYL_LOGO_FILE_PATH))
        .build();
    ChatPostMessageResponse defaultChannelMsgResponse = new ChatPostMessageResponse();
    defaultChannelMsgResponse.setOk(true);
    Mockito.when(mockSlackClient.chatPostMessage(Mockito.eq(defaultChannelMsgRequest))).thenReturn(defaultChannelMsgResponse);

    // Init with a mock slack client.
    SlackNotificationSink sink = new SlackNotificationSink(mockSlackClient);
    sink.init(new NotificationSinkConfig(
        ImmutableMap.of(
            "botToken",
            "abc",
            "defaultChannel",
            "#test"
        ),
        mockSettingsProvider,
        mockIdentityProvider,
        mockSecretProvider,
        mockConnectionService,
        TEST_BASE_URL
    ));

    // Now, construct a message request and verify that it is sent to the slack client.
    NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(new NotificationMessage()
        .setTemplate(com.linkedin.event.notification.template.NotificationTemplateType.valueOf(
            NotificationTemplateType.CUSTOM.name()))
        .setParameters(new StringMap(
            ImmutableMap.of(
                "title",
                "Test Message Title",
                "body",
                "Test Message Body"
            )
        ))
    );

    // Send to a user and a custom channel.
    notificationRequest.setRecipients(new NotificationRecipientArray(
        ImmutableList.of(
          // User recipient
          new NotificationRecipient()
              .setType(NotificationRecipientType.USER)
              .setId(TEST_USER_URN.toString()),
          // Custom Channel Recipient
          new NotificationRecipient()
              .setType(NotificationRecipientType.CUSTOM)
              .setCustomType("SLACK_CHANNEL")
              .setId("#test-channel"),
          // Default Channel Recipient
          new NotificationRecipient()
              .setType(NotificationRecipientType.CUSTOM)
              .setCustomType("SLACK_CHANNEL")
        )
    ));

    // Send the request
    sink.send(notificationRequest, new NotificationContext());

    // Verify that the messages have been sent via the client as expected.
    Mockito.verify(mockSlackClient, Mockito.times(1))
        .usersLookupByEmail(Mockito.eq(lookupRequests));

    Mockito.verify(mockSlackClient, Mockito.times(1))
        .chatPostMessage(Mockito.eq(userMsgRequest));

    Mockito.verify(mockSlackClient, Mockito.times(1))
        .chatPostMessage(Mockito.eq(channelMsgRequest));

    Mockito.verify(mockSlackClient, Mockito.times(1))
        .chatPostMessage(Mockito.eq(defaultChannelMsgRequest));
  }

  @Test
  public void testSendDisabled() throws Exception {

    /*
     * This test verifies that the slack client is not used when the
     * slack sink should be disabled by global settings.
     */
    SettingsProvider mockSettingsProvider = Mockito.mock(SettingsProvider.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings()).thenReturn(
        disabledSettings()
    );
    IdentityProvider mockIdentityProvider = Mockito.mock(IdentityProvider.class);
    SecretProvider mockSecretProvider = Mockito.mock(SecretProvider.class);
    MethodsClient mockSlackClient = Mockito.mock(MethodsClient.class);
    ConnectionService mockConnectionService = Mockito.mock(ConnectionService.class);

    // Init with a mock slack client.
    SlackNotificationSink sink = new SlackNotificationSink(mockSlackClient);
    sink.init(new NotificationSinkConfig(
        ImmutableMap.of(
            "botToken",
            "abc",
            "defaultChannel",
            "#test"
        ),
        mockSettingsProvider,
        mockIdentityProvider,
        mockSecretProvider,
        mockConnectionService,
        "http://localhost:9002"
    ));

    // Now, construct a message request and verify that it is sent to the slack client.
    NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(new NotificationMessage()
        .setTemplate(com.linkedin.event.notification.template.NotificationTemplateType.CUSTOM)
        .setParameters(new StringMap(
            ImmutableMap.of(
                "title",
                "Test Message Title",
                "body",
                "Test Message Body"
            )
        ))
    );

    // Send to a user and a custom channel.
    notificationRequest.setRecipients(new NotificationRecipientArray(
        ImmutableList.of(
            // User recipient
            new NotificationRecipient()
                .setType(NotificationRecipientType.USER)
                .setId(TEST_USER_URN.toString())
        )
    ));

    // Send the request
    sink.send(notificationRequest, new NotificationContext());

    // Verify no invocations of slack client
    Mockito.verify(mockSlackClient, Mockito.times(0))
        .usersLookupByEmail(Mockito.any(UsersLookupByEmailRequest.class));

    Mockito.verify(mockSlackClient, Mockito.times(0))
        .chatPostMessage(Mockito.any(ChatPostMessageRequest.class));
  }

  @Test
  public void testAssertionNotification() throws Exception {
    /*
     * This test verifies that sending a notification for assertion changes
     * makes requests that are expected.
     */
    SettingsProvider mockSettingsProvider = Mockito.mock(SettingsProvider.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings()).thenReturn(
        enabledSettings()
    );
    ConnectionService mockConnectionService = Mockito.mock(ConnectionService.class);
    Mockito.when(mockConnectionService.getConnectionDetails(Mockito.any(Urn.class)))
        .thenReturn(
            null
        );
    IdentityProvider mockIdentityProvider = Mockito.mock(IdentityProvider.class);
    Mockito.when(mockIdentityProvider.getUser(TEST_USER_URN)).thenReturn(testUser());
    SecretProvider mockSecretProvider = Mockito.mock(SecretProvider.class);

    // Init the slack client mock
    MethodsClient mockSlackClient = Mockito.mock(MethodsClient.class);
    UsersLookupByEmailRequest lookupRequests = UsersLookupByEmailRequest.builder()
        .email("test@gmail.com")
        .build();
    UsersLookupByEmailResponse lookupResponse = new UsersLookupByEmailResponse();
    lookupResponse.setOk(true);
    User slackUser = new User();
    slackUser.setId("test-id");
    lookupResponse.setUser(slackUser);
    Mockito.when(mockSlackClient.usersLookupByEmail(Mockito.eq(lookupRequests))).thenReturn(lookupResponse);

    ChatPostMessageRequest dmMsgRequest = ChatPostMessageRequest.builder()
        .channel("12345")
        .text(
            String.format(
                "%s  Assertion now *%s* on *<%s|%s>*", ":white_check_mark:", "passing", "http://localhost:9002/datasets/test/Validation", "SampleName"
            )
        )
        .iconUrl(String.format("http://localhost:9002%s", ACRYL_LOGO_FILE_PATH))
        .build();
    ChatPostMessageResponse defaultChannelMsgResponse = new ChatPostMessageResponse();
    defaultChannelMsgResponse.setOk(true);
    Mockito.when(mockSlackClient.chatPostMessage(Mockito.eq(dmMsgRequest))).thenReturn(defaultChannelMsgResponse);

    // Init with a mock slack client.
    SlackNotificationSink sink = new SlackNotificationSink(mockSlackClient);
    sink.init(new NotificationSinkConfig(
        ImmutableMap.of(
            "botToken",
            "abc",
            "defaultChannel",
            "#test"
        ),
        mockSettingsProvider,
        mockIdentityProvider,
        mockSecretProvider,
        mockConnectionService,
        TEST_BASE_URL
    ));

    // Now, construct an assertion message request and verify that it is sent to the slack client.
    NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(new NotificationMessage()
        .setTemplate(com.linkedin.event.notification.template.NotificationTemplateType.valueOf(
            NotificationTemplateType.BROADCAST_ASSERTION_STATUS_CHANGE.name()))
        .setParameters(new StringMap(
            ImmutableMap.of(
                "entityName",
                "SampleName",
                "entityPath",
                "/datasets/test",
                "result",
                "SUCCESS"
            )
        ))
    );

    notificationRequest.setRecipients(new NotificationRecipientArray(
        ImmutableList.of(
            // Custom DM recipient
            new NotificationRecipient()
                .setType(NotificationRecipientType.CUSTOM)
                .setCustomType("SLACK_DM")
                .setId("12345")
        )
    ));

    // Send the request
    sink.send(notificationRequest, new NotificationContext());

    Mockito.verify(mockSlackClient, Mockito.times(1))
        .chatPostMessage(Mockito.eq(dmMsgRequest));
  }

  @Test
  public void testAssertionNotificationWithRetry() throws Exception {
    /*
     * This test verifies that sending a notification for assertion changes makes
     * requests that are expected and retries up to 3 extra time on rate limit response
     */
    SettingsProvider mockSettingsProvider = Mockito.mock(SettingsProvider.class);
    Mockito.when(mockSettingsProvider.getGlobalSettings()).thenReturn(
        enabledSettings()
    );
    ConnectionService mockConnectionService = Mockito.mock(ConnectionService.class);
    Mockito.when(mockConnectionService.getConnectionDetails(Mockito.any(Urn.class)))
        .thenReturn(
            null
        );
    IdentityProvider mockIdentityProvider = Mockito.mock(IdentityProvider.class);
    Mockito.when(mockIdentityProvider.getUser(TEST_USER_URN)).thenReturn(testUser());
    SecretProvider mockSecretProvider = Mockito.mock(SecretProvider.class);

    // Init the slack client mock
    MethodsClient mockSlackClient = Mockito.mock(MethodsClient.class);
    UsersLookupByEmailRequest lookupRequests = UsersLookupByEmailRequest.builder()
        .email("test@gmail.com")
        .build();
    UsersLookupByEmailResponse lookupResponse = new UsersLookupByEmailResponse();
    lookupResponse.setOk(true);
    User slackUser = new User();
    slackUser.setId("test-id");
    lookupResponse.setUser(slackUser);
    Mockito.when(mockSlackClient.usersLookupByEmail(Mockito.eq(lookupRequests))).thenReturn(lookupResponse);

    ChatPostMessageRequest dmMsgRequest = ChatPostMessageRequest.builder()
        .channel("12345")
        .text(
            String.format(
                "%s  Assertion now *%s* on *<%s|%s>*", ":white_check_mark:", "passing", "http://localhost:9002/datasets/test/Validation", "SampleName"
            )
        )
        .iconUrl(String.format("http://localhost:9002%s", ACRYL_LOGO_FILE_PATH))
        .build();

    Response response = Mockito.mock(Response.class);
    Mockito.when(response.code()).thenReturn(429);
    Mockito.when(response.header("retry-after")).thenReturn("1");
    SlackApiException e = new SlackApiException(response, "");
    Mockito.when(mockSlackClient.chatPostMessage(Mockito.eq(dmMsgRequest))).thenThrow(e);

    // Init with a mock slack client.
    SlackNotificationSink sink = new SlackNotificationSink(mockSlackClient);
    sink.init(new NotificationSinkConfig(
        ImmutableMap.of(
            "botToken",
            "abc",
            "defaultChannel",
            "#test"
        ),
        mockSettingsProvider,
        mockIdentityProvider,
        mockSecretProvider,
        mockConnectionService,
        TEST_BASE_URL
    ));

    // Now, construct an assertion message request and verify that it is sent to the slack client.
    NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(new NotificationMessage()
        .setTemplate(com.linkedin.event.notification.template.NotificationTemplateType.valueOf(
            NotificationTemplateType.BROADCAST_ASSERTION_STATUS_CHANGE.name()))
        .setParameters(new StringMap(
            ImmutableMap.of(
                "entityName",
                "SampleName",
                "entityPath",
                "/datasets/test",
                "result",
                "SUCCESS"
            )
        ))
    );

    notificationRequest.setRecipients(new NotificationRecipientArray(
        ImmutableList.of(
            // Custom DM recipient
            new NotificationRecipient()
                .setType(NotificationRecipientType.CUSTOM)
                .setCustomType("SLACK_DM")
                .setId("12345")
        )
    ));

    // Send the request
    sink.send(notificationRequest, new NotificationContext());

    // should retry a maximum number of 2 time so 3 times total even if we keep hitting the rate limit
    Mockito.verify(mockSlackClient, Mockito.times(3))
        .chatPostMessage(Mockito.eq(dmMsgRequest));
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
    globalIntegrationSettings.setSlackSettings(new SlackIntegrationSettings()
        .setEnabled(false)
    );
    globalSettingsInfo.setIntegrations(globalIntegrationSettings);
    return globalSettingsInfo;
  }

  private GlobalSettingsInfo enabledSettings() {
    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();
    globalSettingsInfo.setNotifications(new GlobalNotificationSettings());

    // Enabled slack settings..
    GlobalIntegrationSettings globalIntegrationSettings = new GlobalIntegrationSettings();
    globalIntegrationSettings.setSlackSettings(new SlackIntegrationSettings()
        .setEnabled(true)
        .setDefaultChannelName("#test")
    );
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