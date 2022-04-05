package com.datahub.notification.slack;

import com.datahub.notification.NotificationContext;
import com.datahub.notification.NotificationSink;
import com.datahub.notification.NotificationSinkConfig;
import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.SecretProvider;
import com.datahub.notification.SettingsProvider;
import com.datahub.notification.IdentityProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.GetMode;
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
   * A list of notification templates supported by this sink.
   */
  private static final List<NotificationTemplateType> SUPPORTED_TEMPLATES = ImmutableList.of(NotificationTemplateType.CUSTOM);
  private static final String SLACK_CHANNEL_RECIPIENT_TYPE = "SLACK_CHANNEL";
  private static final String BOT_TOKEN_CONFIG_NAME = "botToken";
  private static final String DEFAULT_CHANNEL_CONFIG_NAME = "defaultChannel";

  private final Slack slack = Slack.getInstance();
  private final Map<String, User> emailToSlackUser = new HashMap<>();
  private SettingsProvider settingsProvider;
  private IdentityProvider identityProvider;
  private SecretProvider secretProvider;
  private String defaultChannel;
  private String botToken;

  @VisibleForTesting
  MethodsClient slackClient;

  @VisibleForTesting
  SlackNotificationSink(MethodsClient slackClient) {
    this.slackClient = slackClient;
  }

  public SlackNotificationSink() { }

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
    this.identityProvider = cfg.getIdentityProvider();
    this.secretProvider = cfg.getSecretProvider();
    // Optional -- Provide the bot token directly in config. Used until this is available inside UI.
    if (cfg.getStaticConfig().containsKey(BOT_TOKEN_CONFIG_NAME)) {
      botToken = (String) cfg.getStaticConfig().get(BOT_TOKEN_CONFIG_NAME);
    }
    // Optional -- Provide the default channel directly in config. Used until this is available inside UI.
    if (cfg.getStaticConfig().containsKey(DEFAULT_CHANNEL_CONFIG_NAME)) {
      defaultChannel = (String) cfg.getStaticConfig().get(DEFAULT_CHANNEL_CONFIG_NAME);
    }
  }

  @Override
  public void send(
      @Nonnull final NotificationRequest request,
      @Nonnull final NotificationContext context) {
      if (isEnabled()) {
        sendNotifications(request);
      } else {
        log.debug("Skipping sending notification for request {}. Slack sink not enabled.", request);
      }
  }

  /**
   * Returns true if slack notifications are enabled, false otherwise.
   *
   * If Slack integration is ENABLED in Global Settings and a Slack Client can be instantiated,
   * then this method returns true.
   *
   * Instantiation of the slack client simply depends on a "bot token" config being resolvable from global
   * settings or from static configuration.
   */
  private boolean isEnabled() {

    // Fetch application settings, used to determine whether Slack notifications is enabled.
    final GlobalSettingsInfo globalSettings = this.settingsProvider.getGlobalSettings();

    if (globalSettings == null) {
      // Unable to resolve global settings. Cannot determine whether Slack should be enabled. Return disabled.
      log.warn("Unable to resolve global settings. Slack is currently disabled.");
      return false;
    }

    // Next check global settings to determine whether slack is supposed to be enabled.
    if (globalSettings.getIntegrations().hasSlackSettings() && globalSettings.getIntegrations().getSlackSettings().isEnabled()) {
      // Slack is enabled. Let's try to create a slack client (if one doesn't already exist)
      initSlackClient(globalSettings);
      if (this.slackClient != null) {
        return true;
      } else {
        log.error("Slack is enabled, but failed to create slack client! Missing required bot token.");
      }
    }
    // Slack is disabled in settings. Return false.
    return false;
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
    final String title = request.getMessage().getParameters().get("title");
    final String body = request.getMessage().getParameters().get("body");
    final String messageText = String.format("*%s*\n\n%s", title, body);
    sendNotificationToRecipients(request.getRecipients(), messageText);
  }

  private void sendNotificationToRecipients(final List<NotificationRecipient> recipients, final String text) {
    // Send each recipient a message.
    for (NotificationRecipient recipient : recipients) {
      // Try to sink message to each user.
      try {
        if (NotificationRecipientType.USER.equals(recipient.getType())) {
          sendNotificationToUser(UrnUtils.getUrn(recipient.getId()), text);
        } else if (NotificationRecipientType.CUSTOM.equals(recipient.getType()) && SLACK_CHANNEL_RECIPIENT_TYPE.equals(recipient.getCustomType())) {
          // We only support "SLACK_CHANNEL" as a custom type.
          String channel = getRecipientChannelOrDefault(recipient.getId(GetMode.NULL));
          if (channel != null) {
            sendMessage(channel, text);
          } else {
            log.warn(String.format(
                "Failed to resolve channel for recipient of type %s. No default or provided channel.",
                SLACK_CHANNEL_RECIPIENT_TYPE));
          }
        } else {
          throw new UnsupportedOperationException(
              String.format("Failed to send Slack notification. Unsupported recipient type %s provided.", recipient.getType()));
        }
      } catch (Exception e) {
        log.error("Caught exception while attempting to send custom slack notification", e);
      }
    }
  }

  private void sendNotificationToUser(final Urn userUrn, final String text) throws Exception {
    final IdentityProvider.User user = this.identityProvider.getUser(userUrn); // Retrieve DataHub User
    if (user != null && user.getEmail() != null) {
      User slackUser = getSlackUserFromEmail(user.getEmail());
      if (slackUser != null) {
        sendMessage(slackUser.getId(), text);
      }
    } else {
      log.warn(String.format("Failed to send notification to user with urn %s. Failed to find user with valid email in DataHub.", userUrn));
    }
  }

  private void sendMessage(@Nonnull final String channel, @Nonnull final String text) throws Exception {
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
  private String getRecipientChannelOrDefault(@Nullable final String recipientId) {
    return recipientId != null ? recipientId : getDefaultChannelName().orElse(null);
  }

  private Optional<String> getDefaultChannelName() {
    // Resolves a fallback channel to send the notification to, in the case that a channel is not provided.
    // Default channel provided in dynamic settings takes precedence over that provided in static sink config.
    GlobalSettingsInfo globalSettings = this.settingsProvider.getGlobalSettings();
    return globalSettings != null
        && globalSettings.getIntegrations().hasSlackSettings()
        && globalSettings.getIntegrations().getSlackSettings().hasDefaultChannelName()
        ? Optional.ofNullable(globalSettings.getIntegrations().getSlackSettings().getDefaultChannelName())
        : Optional.ofNullable(this.defaultChannel);
  }

  private void initSlackClient(final GlobalSettingsInfo globalSettings) {
    // Attempt to init the slack client from static config or local configuration.
    if (slackClient == null) {
      // Next, attempt to instantiate a slack client using a bot token from static config or settings. Bot token provided in dynamic settings
      // takes precedence over that provided in static sink config.
      if (globalSettings.getIntegrations().hasSlackSettings()
          && globalSettings.getIntegrations().getSlackSettings().hasBotTokenSecret()) {
        try {
          final String botToken = this.secretProvider.getSecretValue(globalSettings.getIntegrations().getSlackSettings().getBotTokenSecret());
          this.slackClient = slack.methods(botToken);
        } catch (Exception e) {
          log.error("Caught exception while attempting to resolve bot token secret. Failed to create slack client.", e);
        }
      } else if (this.botToken != null) {
        // Bot token provided in static configuration.
        this.slackClient = slack.methods(this.botToken);
      } else {
        log.warn("Failed to create Slack client - could not resolve a bot token from static config or global settings!");
      }
    }
  }
}
