package com.datahub.notification.slack;

import com.datahub.notification.NotificationContext;
import com.datahub.notification.NotificationSink;
import com.datahub.notification.NotificationSinkConfig;
import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.SettingsProvider;
import com.datahub.notification.UserProvider;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.slack.api.Slack;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.methods.request.users.UsersLookupByEmailRequest;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.methods.response.users.UsersLookupByEmailResponse;
import com.slack.api.model.User;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * An implementation of {@link com.datahub.notification.NotificationSink} which sends messages to Slack.
 *
 * As configuration, the following is required:
 *
 *    baseUrl (string): the base url where the datahub app is hosted, e.g. https://www.staging.acryl.io
 */
@Slf4j
public class SlackNotificationSink implements NotificationSink {

  /**
   * A list of supported notification templates
   */
  private static final List<NotificationTemplateType> SUPPORTED_TEMPLATES = ImmutableList.of(
      NotificationTemplateType.CUSTOM
  );
  private static final String SLACK_CHANNEL_RECIPIENT_TYPE = "SLACK_CHANNEL";
  private static final String BOT_TOKEN_CONFIG_NAME = "botToken";
  private static final String DEFAULT_CHANNEL_CONFIG_NAME = "defaultChannel";

  private final Slack slack = Slack.getInstance();
  private MethodsClient slackClient;
  private SettingsProvider settingsProvider;
  private UserProvider userProvider;
  private String defaultChannel;
  private String botToken;
  private final Map<String, User> emailToSlackUser = new HashMap<>();

  @Override
  public NotificationSinkType type() {
    return NotificationSinkType.SLACK;
  }

  @Override
  public Collection<NotificationTemplateType> templates() {
    return SUPPORTED_TEMPLATES;
  }

  @Override
  public void init(@Nonnull final NotificationSinkConfig cfg) {
    this.settingsProvider = cfg.getSettingsProvider();
    this.userProvider = cfg.getUserProvider();
    if (cfg.getStaticConfig().containsKey(BOT_TOKEN_CONFIG_NAME)) {
      botToken = (String) cfg.getStaticConfig().get(BOT_TOKEN_CONFIG_NAME);
    }
    if (cfg.getStaticConfig().containsKey(DEFAULT_CHANNEL_CONFIG_NAME)) {
      defaultChannel = (String) cfg.getStaticConfig().get(DEFAULT_CHANNEL_CONFIG_NAME);
    }
  }

  @Override
  public void send(
      @Nonnull final NotificationRequest request,
      @Nonnull final NotificationContext context) {
    CompletableFuture.supplyAsync(() -> {
      if (isEnabled()) {
        // TODO: Determine the best way to handle errors here.
        sendNotifications(request);
      }
      return null;
    });
  }

  private boolean isEnabled() {
    // First, check to ensure global settings have not been disabled for slack.
    final GlobalSettingsInfo globalSettings = this.settingsProvider.getGlobalSettings();
    if (globalSettings.getIntegrations().hasSlackSettings() && !globalSettings.getIntegrations().getSlackSettings().isEnabled()) {
      return false;
    }

    // Next, verify that we're able to create a slack client.
    tryInitSlackClient(globalSettings);

    // Only enable if the slack client could be successfully created.
    return this.slackClient != null;
  }

  private void sendNotifications(final NotificationRequest notificationRequest) {
    final NotificationTemplateType templateType = NotificationTemplateType.valueOf(notificationRequest.getMessage().getTemplate());
    switch (templateType) {
      case CUSTOM:
        sendCustomNotification(notificationRequest);
        break;
      default:
        throw new UnsupportedOperationException(String.format(
            "Unsupported template type %s providing to %s",
            templateType,
            this.getClass().getCanonicalName()));
    }
  }

  private void sendCustomNotification(final NotificationRequest request) {
    sendNotificationToRecipients(request.getRecipients(), request.getMessage().getParameters().get("body"));
  }

  private void sendNotificationToRecipients(final List<NotificationRecipient> recipients, final String text) {
    // Send each recipient a message.
    for (NotificationRecipient recipient : recipients) {
      // Try to sink message to each user.
      try {
        if (NotificationRecipientType.USER.equals(recipient.getType())) {
          sendNotificationToUser(UrnUtils.getUrn(recipient.getId()), text);
        } else if (NotificationRecipientType.CUSTOM.equals(recipient.getType()) && SLACK_CHANNEL_RECIPIENT_TYPE.equals(recipient.getCustomRecipientType())) {
          // We only support "SLACK_CHANNEL" as a custom type.
          String channel = getRecipientChannelOrDefault(recipient.getId());
          sendMessage(channel, text);
        } else {
          throw new UnsupportedOperationException(
              String.format("Failed to send Slack notification. Unsupported recipient type %s provided.", recipient.getType()));
        }
      } catch (Exception e) {
        log.error("Caught exception while attempting to send custom slack notification", e);
      }
    }
    // todo: figure out the failure story.
  }

  private void sendNotificationToUser(final Urn userUrn, final String text) throws Exception {
    final UserProvider.User user = this.userProvider.getUser(userUrn); // Retrieve DataHub User
    if (user != null && user.getEmail() != null) {
      User slackUser = getSlackUserFromEmail(user.getEmail());
      if (slackUser != null) {
        sendMessage(slackUser.getId(), text);
      }
    } else {
      log.warn(String.format("Failed to send notification to user with urn %s. Failed to find user with valid email in DataHub.", userUrn));
    }
  }

  private void sendMessage(final String channel, final String text) throws Exception {
    final ChatPostMessageRequest msgRequest = ChatPostMessageRequest.builder()
        .channel(channel)
        .text(text)
        .build();
    final ChatPostMessageResponse response = sendMessage(msgRequest);
    if (response.isOk()) {
      log.debug(String.format("Successfully sent Slack notification to channel %s", channel));
    } else {
      log.error(String.format("Failed to sink Slack notification to channel %s. Received error from Slack API: %s", channel, response.getError()));
    }
  }

  private ChatPostMessageResponse sendMessage(final ChatPostMessageRequest request) throws Exception {
    try {
      return slackClient.chatPostMessage(request);
    } catch (IOException | SlackApiException e) {
      throw new Exception("Caught exception while attempting to send slack message", e);
    }
  }

  @Nullable
  private User getSlackUserFromEmail(@Nonnull final String email) throws Exception {
    if (this.emailToSlackUser.containsKey(email)) {
      // Then return this
      return this.emailToSlackUser.get(email);
    } else {
      final UsersLookupByEmailResponse response = getSlackUserLookupResponseFromEmail(email);
      if (response.isOk()) {
        User slackUser = response.getUser();
        this.emailToSlackUser.put(email, slackUser); // Store in cache.
        return slackUser;
      } else {
        log.warn(String.format("Received API error while attempting to resolve a Slack user with email %s. Error: %s",
            email,
            response.getError()));
      }
    }
    return null;
  }

  private UsersLookupByEmailResponse getSlackUserLookupResponseFromEmail(@Nonnull final String email) throws Exception {
    final UsersLookupByEmailRequest request = UsersLookupByEmailRequest.builder()
        .email(email)
        .build();
    try {
      return slackClient.usersLookupByEmail(request);
    } catch (IOException | SlackApiException e) {
      throw new Exception("Caught exception while attempting to lookup slack user by email", e);
    }
  }

  @Nullable
  private String getRecipientChannelOrDefault(final String recipientId) {
    return recipientId != null ? recipientId : getDefaultChannelName().orElse(null);
  }

  private Optional<String> getDefaultChannelName() {
    GlobalSettingsInfo globalSettings = this.settingsProvider.getGlobalSettings();
    return globalSettings != null && globalSettings.getIntegrations().hasSlackSettings()
        ? Optional.ofNullable(globalSettings.getIntegrations().getSlackSettings().getDefaultChannelName())
        : Optional.ofNullable(this.defaultChannel);
  }

  private void tryInitSlackClient(final GlobalSettingsInfo globalSettings) {
    // Attempt to init the slack client from static config or local configuration.
    if (slackClient == null) {
      // Next, attempt to instantiate a slack client using a bot token from static config or settings. Bot token provided in static configs takes precedence
      // because its simpler.
      if (this.botToken != null) {
        // Bot token provided in static configuration.
        this.slackClient = slack.methods(this.botToken);
      } else if (globalSettings.getIntegrations().hasSlackSettings()
          && globalSettings.getIntegrations().getSlackSettings().isEnabled()
          && globalSettings.getIntegrations().getSlackSettings().hasBotTokenSecret()) {
        // TODO: Resolve secret from the bot token secret. For now just use the URN part.
        final String botToken = getBotToken(globalSettings.getIntegrations().getSlackSettings().getBotTokenSecret());
        this.slackClient = slack.methods(botToken);
      } else {
        log.warn("Failed to build Slack client - failed to resolve a bot token!");
      }
    }
  }

  private String getBotToken(final Urn botTokenSecret) {
    // TODO: Fetch from encrypted secret. For time being, return raw string.
    return botTokenSecret.getId();
  }
}
