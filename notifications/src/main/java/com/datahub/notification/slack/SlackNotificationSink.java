package com.datahub.notification.slack;

import com.datahub.notification.NotificationContext;
import com.datahub.notification.NotificationSink;
import com.datahub.notification.NotificationSinkConfig;
import com.datahub.notification.NotificationSinkResult;
import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.SettingsProvider;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.slack.api.Slack;
import com.slack.api.methods.MethodsClient;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
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
      // TODO
  );
  private static final String SLACK_CHANNEL_RECIPIENT_TYPE = "SLACK_CHANNEL";
  private static final String BOT_TOKEN_CONFIG_NAME = "botToken";
  private static final String DEFAULT_CHANNEL_CONFIG_NAME = "defaultChannel";

  private final Slack slackClient = Slack.getInstance();
  private MethodsClient methodsClient;
  private SettingsProvider settingsProvider;
  private String defaultChannel;

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
    if (cfg.getStaticConfig().containsKey(BOT_TOKEN_CONFIG_NAME)) {
      final String botToken = (String) cfg.getStaticConfig().get(BOT_TOKEN_CONFIG_NAME);
      methodsClient = slackClient.methods(botToken);
    }
    if (cfg.getStaticConfig().containsKey(BOT_TOKEN_CONFIG_NAME)) {
      final String botToken = (String) cfg.getStaticConfig().get(BOT_TOKEN_CONFIG_NAME);
      methodsClient = slackClient.methods(botToken);
    }
    if (cfg.getStaticConfig().containsKey(DEFAULT_CHANNEL_CONFIG_NAME)) {
      defaultChannel = (String) cfg.getStaticConfig().get(DEFAULT_CHANNEL_CONFIG_NAME);
    }
  }

  @Override
  public CompletableFuture<NotificationSinkResult> send(
      @Nonnull final NotificationRequest request,
      @Nonnull final NotificationContext context) {
    return CompletableFuture.supplyAsync(() -> {
      if (isEnabled()) {
        return sendNotifications(request);
      } else {
        return new NotificationSinkResult(
            NotificationSinkResult.Result.SKIP,
            NotificationSinkResult.Reason.DISABLED_SINK,
            null);
      }
    });
  }

  private NotificationSinkResult sendNotifications(final NotificationRequest notificationRequest) {
    // TODO
    final NotificationTemplateType templateType = NotificationTemplateType.valueOf(notificationRequest.getMessage().getTemplate());
    switch (templateType) {
      default:
        throw new UnsupportedOperationException(String.format(
            "Unsupported template type %s providing to %s",
            templateType,
            this.getClass().getCanonicalName()));
    }
  }

  private boolean isEnabled() {
    // For now, always check global settings object to determine whether Slack is enabled.

    GlobalSettingsInfo globalSettings = this.settingsProvider.getGlobalSettings();
    // Init the methods client if necessary - based on global settings.
    if (globalSettings != null && globalSettings.getIntegrations().hasSlackSettings() && globalSettings.getIntegrations().getSlackSettings().isEnabled()) {
      if (this.methodsClient == null && globalSettings.getIntegrations().getSlackSettings().getBotTokenSecret() != null) {
        final String botToken = getBotToken(globalSettings.getIntegrations().getSlackSettings().getBotTokenSecret());
        this.methodsClient = slackClient.methods(botToken);
      }
    }

    return methodsClient != null
        && globalSettings != null
        && globalSettings.getIntegrations().hasSlackSettings()
        && globalSettings.getIntegrations().getSlackSettings().isEnabled();
  }

  private Optional<String> getDefaultChannelName() {
    GlobalSettingsInfo globalSettings = this.settingsProvider.getGlobalSettings();
    return globalSettings != null && globalSettings.getIntegrations().hasSlackSettings()
        ? Optional.ofNullable(globalSettings.getIntegrations().getSlackSettings().getDefaultChannelName())
        : Optional.ofNullable(this.defaultChannel);
  }

  private String getBotToken(final Urn botTokenSecret) {
    // TODO: Fetch from encrypted secret. For time being, return raw string.
    return botTokenSecret.getId();
  }
}
