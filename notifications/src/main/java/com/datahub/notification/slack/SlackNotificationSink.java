package com.datahub.notification.slack;

import com.datahub.notification.NotificationContext;
import com.datahub.notification.NotificationSink;
import com.datahub.notification.NotificationSinkConfig;
import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.provider.SecretProvider;
import com.datahub.notification.provider.SettingsProvider;
import com.datahub.notification.provider.IdentityProvider;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.datahub.notification.NotificationUtils.*;


/**
 * An implementation of {@link com.datahub.notification.NotificationSink} which sends messages to Slack.
 *
 * As configuration, the following is required:
 *
 *    baseUrl (string): the base url where the datahub app is hosted, e.g. https://www.staging.acryl.io
 *
 * # TODO: Make this a templatized Notification Sink.
 */
@Slf4j
public class SlackNotificationSink implements NotificationSink {

  /**
   * A list of notification templates supported by this sink.
   */
  private static final List<NotificationTemplateType> SUPPORTED_TEMPLATES = ImmutableList.of(
      NotificationTemplateType.CUSTOM,
      NotificationTemplateType.BROADCAST_NEW_INCIDENT,
      NotificationTemplateType.BROADCAST_INCIDENT_STATUS_CHANGE,
      NotificationTemplateType.BROADCAST_NEW_PROPOSAL,
      NotificationTemplateType.BROADCAST_PROPOSAL_STATUS_CHANGE,
      NotificationTemplateType.BROADCAST_ENTITY_CHANGE,
      NotificationTemplateType.BROADCAST_INGESTION_RUN_CHANGE
  );
  private static final String SLACK_CHANNEL_RECIPIENT_TYPE = "SLACK_CHANNEL";
  private static final String BOT_TOKEN_CONFIG_NAME = "botToken";
  private static final String DEFAULT_CHANNEL_CONFIG_NAME = "defaultChannel";

  private final Slack slack = Slack.getInstance();
  private final Map<String, User> emailToSlackUser = new HashMap<>();
  private SettingsProvider settingsProvider;
  private IdentityProvider identityProvider;
  private SecretProvider secretProvider;
  private String baseUrl;
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
    this.baseUrl = cfg.getBaseUrl();
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
      case BROADCAST_NEW_INCIDENT:
        sendBroadcastNotification(notificationRequest.getRecipients(), buildNewIncidentMessage(notificationRequest));
        break;
      case BROADCAST_INCIDENT_STATUS_CHANGE:
        sendBroadcastNotification(notificationRequest.getRecipients(), buildIncidentStatusChangeMessage(notificationRequest));
        break;
      case BROADCAST_NEW_PROPOSAL:
        sendBroadcastNotification(notificationRequest.getRecipients(), buildNewProposalMessage(notificationRequest));
        break;
      case BROADCAST_PROPOSAL_STATUS_CHANGE:
        sendBroadcastNotification(notificationRequest.getRecipients(), buildProposalStatusChangeMessage(notificationRequest));
        break;
      case BROADCAST_ENTITY_CHANGE:
        sendBroadcastNotification(notificationRequest.getRecipients(), buildEntityChangeMessage(notificationRequest));
        break;
      case BROADCAST_INGESTION_RUN_CHANGE:
        sendBroadcastNotification(notificationRequest.getRecipients(), buildIngestionRunChangeMessage(notificationRequest));
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

  private String buildEntityChangeMessage(NotificationRequest request) {
    final String actorName = getUserName(request.getMessage().getParameters().get("actorUrn"));
    final String entityName = request.getMessage().getParameters().get("entityName");
    final String entityUrl = String.format("%s%s", this.baseUrl, request.getMessage().getParameters().get("entityPath"));
    final String entityType = request.getMessage().getParameters().get("entityType");
    final String operation = request.getMessage().getParameters().get("operation");
    final String modifierType = request.getMessage().getParameters().get("modifierType");
    final String modifierStr = buildEntityChangeModifierString(request.getMessage().getParameters());

    // TODO: Handle Sub-resources (fields, etc)
    /*
     * Example:
     *     - John Joyce has added tag(s) PII, Test, Test2, + 3 more for SampleKafkaDataset.
     *     - John Joyce has updated deprecation for SampleKafkaDataset.
     *     - John Joyce has removed glossary term(s) a, b for SampleKafkaDataset.
     */
    return String.format(">:pencil2:  *%s* has %s %s%s for %s *<%s|%s>*.",
        actorName,
        operation,
        modifierType,
        modifierStr,
        entityType,
        entityUrl,
        entityName
    );
  }

  private String buildEntityChangeModifierString(Map<String, String> params) {
    // Handle multiple modifiers.
    final Integer modifierCount = params.get("modifierCount") != null ? Integer.valueOf(params.get("modifierCount")) : null;
    if (modifierCount != null && modifierCount > 0) {
      // There are modifiers.
      StringBuilder builder = new StringBuilder(" ");
      for (int i = 0; i < modifierCount && i < 3; i++) {
        // For each modifier, add it to a stringbuilder
        String modifierName = params.get(String.format("modifier%sName", i));
        String modifierPath = params.get(String.format("modifier%sPath", i));
        String modifierUrl = String.format("%s%s", this.baseUrl, modifierPath);
        builder.append(String.format("*<%s|%s>*", modifierUrl, modifierName));
        if (i < modifierCount - 1) {
          builder.append(", ");
        }
      }
      if (modifierCount > 3) {
        // Then add + x more as the end. By default we only show the first 3.
        builder.append(String.format("+ %s more", modifierCount - 3));
      }
      return builder.toString();
    }
    return "";
  }

  private String buildNewProposalMessage(NotificationRequest request) {
    final String actorName = getUserName(request.getMessage().getParameters().get("actorUrn"));
    final String entityName = request.getMessage().getParameters().get("entityName");
    final String entityUrl = String.format("%s%s", this.baseUrl, request.getMessage().getParameters().get("entityPath"));
    final String modifierUrl = String.format("%s%s", this.baseUrl, request.getMessage().getParameters().get("modifierPath"));
    final String entityType = request.getMessage().getParameters().get("entityType");
    final String modifierType = request.getMessage().getParameters().get("modifierType");
    final String modifierName = request.getMessage().getParameters().get("modifierName");

    final String maybeSubResourceType = request.getMessage().getParameters().get("subResourceType");
    final String maybeSubResource = request.getMessage().getParameters().get("subResource");

    if (maybeSubResource != null && maybeSubResourceType != null) {
      /*
       * Examples:
       *
       *     - John Joyce has proposed tag PII for schema field foo of SampleKafkaDataset.
       *     - John Joyce has proposed glossary term FOOBAR for schema field bar of SampleKafkaDataset.
       */
      return String.format(":incoming_envelope: *New Proposal Raised*\n\n*%s* has proposed %s *<%s|%s>* for *%s* of %s *<%s|%s>*.",
          actorName,
          modifierType,
          modifierUrl,
          modifierName,
          maybeSubResource,
          entityType,
          entityUrl,
          entityName
      );
    }
    /*
     * Examples:
     *
     *     - John Joyce has proposed tag PII for SampleKafkaDataset.
     *     - John Joyce has proposed glossary term FOOBAR for SampleKafkaDataset.
     */
    return String.format(":incoming_envelope: *New Proposal Raised*\n\n*%s* has proposed %s *<%s|%s>* for %s *<%s|%s>*.",
        actorName,
        modifierType,
        modifierUrl,
        modifierName,
        entityType,
        entityUrl,
        entityName
    );
  }

  private String buildProposalStatusChangeMessage(NotificationRequest request) {
    // Fetch each user's email, this is required to understand their slack ids.
    final String actorName = getUserName(request.getMessage().getParameters().get("actorUrn"));
    final String entityName = request.getMessage().getParameters().get("entityName");
    final String entityUrl = String.format("%s%s", this.baseUrl, request.getMessage().getParameters().get("entityPath"));
    final String modifierUrl = String.format("%s%s", this.baseUrl, request.getMessage().getParameters().get("modifierPath"));
    final String entityType = request.getMessage().getParameters().get("entityType");
    final String modifierType = request.getMessage().getParameters().get("modifierType");
    final String modifierName = request.getMessage().getParameters().get("modifierName");
    final String operation = request.getMessage().getParameters().get("operation");
    final String action = request.getMessage().getParameters().get("action");

    final String maybeSubResourceType = request.getMessage().getParameters().get("subResourceType");
    final String maybeSubResource = request.getMessage().getParameters().get("subResource");

    if (maybeSubResource != null && maybeSubResourceType != null) {
      return String.format(":incoming_envelope: *Proposal Status Changed*\n\n*%s* has %s proposal to %s %s *<%s|%s>* for *%s* of %s *<%s|%s>*.",
          actorName,
          action,
          operation,
          modifierType,
          modifierUrl,
          modifierName,
          maybeSubResource,
          entityType,
          entityUrl,
          entityName
      );
    }
    return String.format(":incoming_envelope: *Proposal Status Changed*\n\n*%s* has %s proposal to %s %s *<%s|%s>* for %s *<%s|%s>*.",
        actorName,
        action,
        operation,
        modifierType,
        modifierUrl,
        modifierName,
        entityType,
        entityUrl,
        entityName
    );
  }

  private String buildNewIncidentMessage(NotificationRequest request) {

    // Extract owner urns, downstream owner urns.
    final List<Urn> ownerUrns = jsonToStrList(request.getMessage()
        .getParameters()
        .get("owners"))
        .stream()
        .map(UrnUtils::getUrn)
        .collect(Collectors.toList());

    final List<Urn> downstreamOwnerUrns = jsonToStrList(request.getMessage()
        .getParameters()
        .get("downstreamOwners"))
        .stream()
        .map(UrnUtils::getUrn)
        .collect(Collectors.toList());

    // Fetch each user's email, this is required to understand their slack ids.
    final Set<Urn> allUsers = new HashSet<>();
    allUsers.addAll(ownerUrns);
    allUsers.addAll(downstreamOwnerUrns);
    Map<Urn, IdentityProvider.User> users = Collections.emptyMap();
    try {
      users = this.identityProvider.batchGetUsers(
          allUsers
      );
    } catch (Exception e) {
      // If we cannot resolve the users, still broadcast the message.
      log.warn("Failed to resolve users from GMS. Skipping tagging them in Slack broadcast.");
    }

    // Build the message.
    // TODO: Replace this with a template DSL (e.g. Jinja)
    final String url = String.format("%s%s", this.baseUrl, request.getMessage().getParameters().get("entityPath"));
    final String title = request.getMessage().getParameters().get("incidentTitle");
    final String description = request.getMessage().getParameters().get("incidentDescription");
    final String actorName = getUserName(request.getMessage().getParameters().get("actorUrn"));
    final String ownersStr = createUsersTagString(
        users.keySet()
            .stream()
            .filter(ownerUrns::contains)
            .map(users::get)
            .collect(Collectors.toList()));
    final String downstreamOwnersStr = createUsersTagString(
        users.keySet()
            .stream()
            .filter(downstreamOwnerUrns::contains)
            .map(users::get)
            .collect(Collectors.toList()));

    return String.format("%s%s",
        String.format(":warning: *New Incident Raised* \n\nA new incident has been raised on asset %s%s.",
            url,
            actorName != null ? String.format(" by *%s*", actorName) : ""),
        String.format("\n\n*Incident Name*: %s\n*Incident Description*: %s\n\n*Asset Owners*: %s\n*Downstream Asset Owners*: %s",
            title != null ? title : "None",
            description != null ? description : "None",
            ownersStr.length() > 0 ? ownersStr : "N/A",
            downstreamOwnersStr.length() > 0 ? downstreamOwnersStr : "N/A"
        )
    );
  }

  private String buildIncidentStatusChangeMessage(NotificationRequest request) {
    final List<Urn> ownerUrns = jsonToStrList(request.getMessage()
        .getParameters()
        .get("owners"))
        .stream()
        .map(UrnUtils::getUrn)
        .collect(Collectors.toList());

    final List<Urn> downstreamOwnerUrns = jsonToStrList(request.getMessage()
        .getParameters()
        .get("downstreamOwners"))
        .stream()
        .map(UrnUtils::getUrn)
        .collect(Collectors.toList());

    // Fetch each user's email, this is required to understand their slack ids.
    final Set<Urn> allUsers = new HashSet<>();
    allUsers.addAll(ownerUrns);
    allUsers.addAll(downstreamOwnerUrns);
    Map<Urn, IdentityProvider.User> users = Collections.emptyMap();
    try {
      users = this.identityProvider.batchGetUsers(
          allUsers
      );
    } catch (Exception e) {
      log.warn("Failed to resolve users from GMS. Skipping adding them to notification.");
    }

    // Build the message. TODO: Use a template here.
    final String url = String.format("%s%s", this.baseUrl, request.getMessage().getParameters().get("entityPath"));
    final String message = request.getMessage().getParameters().get("message");
    final String title = request.getMessage().getParameters().get("incidentTitle");
    final String description = request.getMessage().getParameters().get("incidentDescription");
    final String prevStatus = request.getMessage().getParameters().get("prevStatus");
    final String newStatus = request.getMessage().getParameters().get("newStatus");
    final String actorName = getUserName(request.getMessage().getParameters().get("actorUrn"));
    final String ownersStr = createUsersTagString(
        users.keySet()
            .stream()
            .filter(ownerUrns::contains)
            .map(users::get)
            .collect(Collectors.toList()));
    final String downstreamOwnersStr = createUsersTagString(
        users.keySet()
            .stream()
            .filter(downstreamOwnerUrns::contains)
            .map(users::get)
            .collect(Collectors.toList()));

    final String icon = newStatus.equals("RESOLVED") ? ":white_check_mark:" : ":warning:";
    return String.format("%s%s%s",
        String.format("%s *Incident Status Changed*\n\n The status of incident *%s* on asset %s has changed from *%s* to *%s*%s.",
            icon,
            title != null ? title : "None",
            url,
            prevStatus,
            newStatus,
            actorName != null ? String.format(" by *%s*", actorName) : ""),
        String.format("\n\n *Message*: %s\n",
            message != null ? message : "None"
        ),
        String.format("\n\n*Incident Name*: %s\n*Incident Description*: %s\n\n*Asset Owners*: %s\n*Downstream Asset Owners*: %s",
            title != null ? title : "None",
            description != null ? description : "None",
            ownersStr.length() > 0 ? ownersStr : "N/A",
            downstreamOwnersStr.length() > 0 ? downstreamOwnersStr : "N/A"
        )
    );
  }

  private String buildIngestionRunChangeMessage(NotificationRequest request) {
    final String sourceName = request.getMessage().getParameters().get("sourceName");
    final String sourceType = request.getMessage().getParameters().get("sourceType");
    final String statusText = request.getMessage().getParameters().get("statusText");
    final String ingestionUrl = String.format("%s%s", this.baseUrl, "/ingestion");

    /*
     * Example:
     *     - Ingestion source my-ingestion-source of type kafka has failed!
     *     - Ingestion source my-ingestion-source of type bigquery-usage has completed!
     *     - Ingestion source my-ingestion-source of type looker has been cancelled!
     *     - Ingestion source my-ingestion-source of type okta has timed out!
     *     - Ingestion source my-ingestion-source of type snowflake has started!
     */
    return String.format(">:electric_plug:   Ingestion source *%s* of type *%s* has %s! <%s|View ingestion sources>.",
        sourceName,
        sourceType,
        statusText,
        ingestionUrl
    );
  }

  private void sendNotificationToRecipients(final List<NotificationRecipient> recipients, final String text) {
    // Send each recipient a message.
    for (NotificationRecipient recipient : recipients) {
      sendNotificationToRecipient(recipient, text);
    }
  }

  private void sendNotificationToRecipient(final NotificationRecipient recipient, final String text) {
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

  private void sendBroadcastNotification(final List<NotificationRecipient> recipients, final String text) {
    // In the case of a broadcast, if there are no recipients explicitly provided we fallback to sending to the default configured channel.
    if (recipients.size() > 0) {
      // Send to each recipient in the list as normal.
      sendNotificationToRecipients(recipients, text);
    } else {
      // Broadcast to the default configured channel.
      NotificationRecipient defaultChannelRecipient = new NotificationRecipient()
          .setType(NotificationRecipientType.CUSTOM)
          .setCustomType(SLACK_CHANNEL_RECIPIENT_TYPE);
      sendNotificationToRecipient(defaultChannelRecipient, text);
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

  private String createUsersTagString(final List<IdentityProvider.User> users) {
    // Resolve a User object to their slack handle.
    StringBuilder tagString = new StringBuilder();
    for (IdentityProvider.User user : users) {
      String userTagString = createUserTagString(user);
      if (userTagString != null) {
        tagString.append(String.format("%s ", userTagString));
      }
    }
    return tagString.toString();
  }

  /**
   * Returns a formatted slack user tag string, e.g. <@JohnJoyce> if the user can be resolved to a slack id, null if not.
   */
  @Nullable
  private String createUserTagString(final IdentityProvider.User user) {
    // Resolve a User object to their slack handle.
    if (user.getEmail() != null) {
      try {
        User slackUser = getSlackUserFromEmail(user.getEmail());
        // Add the slack user to the string.
        if (slackUser != null) {
          return String.format("<@%s>", slackUser.getId());
        } else {
          log.warn(
              String.format("Skipping adding user with email %s to tag string. No corresponding slack user found.", user.getEmail()));
        }
      } catch (Exception e) {
        log.error(String.format(
            "Caught exception while attempting to resolve user with email %s to slack user. Skipping adding user to tag string.", user.getEmail()), e);
      }
    } else {
      log.warn("Failed to resolve DataHub user to slack user by email. No email found for user!");
    }
    return null;
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

  @Nullable
  private String getUserName(final String userUrnStr) {
    try {
      Urn userUrn = Urn.createFromString(userUrnStr);
      IdentityProvider.User user = this.identityProvider.getUser(userUrn);
      return user != null ? user.getResolvedDisplayName() : null;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Invalid actor urn %s provided", userUrnStr));
    }
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
          && globalSettings.getIntegrations().getSlackSettings().hasEncryptedBotToken()) {
        try {
          final String botToken = this.secretProvider.decryptSecret(globalSettings.getIntegrations().getSlackSettings().getEncryptedBotToken());
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
