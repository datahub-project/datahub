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
import com.linkedin.data.template.StringMap;
import com.linkedin.event.notification.NotificationMessage;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientArray;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.settings.global.GlobalIntegrationSettings;
import com.linkedin.settings.global.GlobalNotificationSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.SlackIntegrationSettings;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.methods.request.users.UsersLookupByEmailRequest;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.methods.response.users.UsersLookupByEmailResponse;
import com.slack.api.model.User;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.annotations.Test;

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

    // Case 1: Empty Static Config
    sink.init(new NotificationSinkConfig(
        Collections.emptyMap(),
        mockSettingsProvider,
        mockIdentityProvider,
        mockSecretProvider,
        TEST_BASE_URL
    ));

    // Case 2: Static config with bot token and default channel
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
        TEST_BASE_URL
    ));
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
        .build();
    ChatPostMessageResponse usrMsgResponse = new ChatPostMessageResponse();
    usrMsgResponse.setOk(true);
    Mockito.when(mockSlackClient.chatPostMessage(Mockito.eq(userMsgRequest))).thenReturn(usrMsgResponse);

    ChatPostMessageRequest channelMsgRequest = ChatPostMessageRequest.builder()
        .channel("#test-channel")
        .text("*Test Message Title*\n\nTest Message Body")
        .build();
    ChatPostMessageResponse channelMsgResponse = new ChatPostMessageResponse();
    channelMsgResponse.setOk(true);
    Mockito.when(mockSlackClient.chatPostMessage(Mockito.eq(channelMsgRequest))).thenReturn(channelMsgResponse);

    ChatPostMessageRequest defaultChannelMsgRequest = ChatPostMessageRequest.builder()
        .channel("#test")
        .text("*Test Message Title*\n\nTest Message Body")
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
        TEST_BASE_URL
    ));

    // Now, construct a message request and verify that it is sent to the slack client.
    NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(new NotificationMessage()
        .setTemplate(NotificationTemplateType.CUSTOM.name())
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
        "http://localhost:9002"
    ));

    // Now, construct a message request and verify that it is sent to the slack client.
    NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(new NotificationMessage()
        .setTemplate(NotificationTemplateType.CUSTOM.name())
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
    );
    globalSettingsInfo.setIntegrations(globalIntegrationSettings);
    return globalSettingsInfo;
  }
}