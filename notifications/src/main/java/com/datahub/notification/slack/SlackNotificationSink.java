package com.datahub.notification.slack;

import static com.datahub.notification.NotificationUtils.*;
import static com.linkedin.metadata.AcrylConstants.*;
import static com.linkedin.metadata.Constants.CORP_GROUP_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR_NAME;

import com.datahub.notification.NotificationContext;
import com.datahub.notification.NotificationSink;
import com.datahub.notification.NotificationSinkConfig;
import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.provider.EntityNameProvider;
import com.datahub.notification.provider.IdentityProvider;
import com.datahub.notification.provider.SecretProvider;
import com.datahub.notification.provider.SettingsProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.connection.DataHubConnectionDetails;
import com.linkedin.connection.DataHubConnectionDetailsType;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.StructuredExecutionReport;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.connection.ConnectionService;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.service.util.AssertionUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.slack.api.Slack;
import com.slack.api.SlackConfig;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.methods.request.users.UsersLookupByEmailRequest;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.methods.response.users.UsersLookupByEmailResponse;
import com.slack.api.model.User;
import io.datahubproject.integrations.invoker.JSON;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * An implementation of {@link com.datahub.notification.NotificationSink} which sends messages to
 * Slack.
 *
 * <p>As configuration, the following is required:
 *
 * <p>baseUrl (string): the base url where the datahub app is hosted, e.g.
 * https://www.staging.acryl.io
 *
 * <p># TODO: Make this a templatized Notification Sink.
 */
@Slf4j
public class SlackNotificationSink implements NotificationSink {
  private static final String DEPRECATION_MODIFIER_TYPE = "deprecation";
  static final Urn SLACK_CONNECTION_URN = UrnUtils.getUrn(Constants.SLACK_CONNECTION_ID);

  /** A list of notification templates supported by this sink. */
  private static final List<NotificationTemplateType> ALL_SUPPORTED_TEMPLATES =
      ImmutableList.of(
          NotificationTemplateType.CUSTOM,
          NotificationTemplateType.BROADCAST_NEW_INCIDENT,
          NotificationTemplateType.BROADCAST_INCIDENT_STATUS_CHANGE,
          NotificationTemplateType.BROADCAST_NEW_PROPOSAL,
          NotificationTemplateType.BROADCAST_PROPOSAL_STATUS_CHANGE,
          NotificationTemplateType.BROADCAST_ENTITY_CHANGE,
          NotificationTemplateType.BROADCAST_INGESTION_RUN_CHANGE,
          NotificationTemplateType.BROADCAST_ASSERTION_STATUS_CHANGE);

  /** Templates that are supported by the V2 sink, and may be skipped by this sink. */
  private static final List<NotificationTemplateType> V2_SUPPORTED_TEMPLATES =
      ImmutableList.of(
          NotificationTemplateType.BROADCAST_NEW_INCIDENT,
          NotificationTemplateType.BROADCAST_INCIDENT_STATUS_CHANGE);

  /** A list of recipient types that can be handled by the sink */
  private static final List<NotificationRecipientType> RECIPIENT_TYPES =
      ImmutableList.of(NotificationRecipientType.SLACK_CHANNEL, NotificationRecipientType.SLACK_DM);

  // TODO: consolidate this fricken duplicated mess
  private static final String BOT_TOKEN_CONFIG_NAME = "botToken";
  private static final String RETRY_ENABLED_CONFIG_NAME = "retryEnabled";
  private static final String MAX_NUM_RETRIES_CONFIG_NAME = "maxNumRetries";
  private static final String DEFAULT_CHANNEL_CONFIG_NAME = "defaultChannel";
  private static final String PROXY_URL_CONFIG_NAME = "proxyUrl";
  private static final String SLACK_SINK_V2_ENABLED = "slackSinkV2Enabled";
  private static final String NOTIFICATION_DROPPED_METRIC = "slack_notification_dropped";
  private static final String RETRY_AFTER_HEADER = "retry-after";
  private static final int RATE_LIMIT_ERROR_CODE = 429;

  enum RetryMode {
    ENABLED,
    DISABLED
  }

  private final Map<String, User> emailToSlackUser = new HashMap<>();
  private Slack slack;
  private EntityClient entityClient;
  private SettingsProvider settingsProvider;
  private IdentityProvider identityProvider;
  private EntityNameProvider entityNameProvider;
  private SecretProvider secretProvider;
  private ConnectionService connectionService;
  private String baseUrl;
  private String staticConfigDefaultChannel;
  private String staticConfigBotToken;
  private boolean retryEnabled;
  private Integer maxRetries;
  private long retryAfterTimestamp;
  private boolean slackSinkV2Enabled = false;

  @VisibleForTesting String botToken;

  @VisibleForTesting MethodsClient slackClient;

  @VisibleForTesting
  SlackNotificationSink(MethodsClient slackClient) {
    this.slackClient = slackClient;
  }

  @VisibleForTesting
  SlackNotificationSink(Slack slack) {
    this.slack = slack;
  }

  public SlackNotificationSink() {}

  @Override
  public NotificationSinkType type() {
    return NotificationSinkType.SLACK;
  }

  @Override
  public Collection<NotificationTemplateType> templates() {
    // If the V2 sink is enabled, then we should not support the V2 templates.
    // Otherwise, we should support all templates.
    return slackSinkV2Enabled
        ? ALL_SUPPORTED_TEMPLATES.stream()
            .filter(template -> !V2_SUPPORTED_TEMPLATES.contains(template))
            .collect(Collectors.toList())
        : ALL_SUPPORTED_TEMPLATES;
  }

  @Override
  public Collection<NotificationRecipientType> recipientTypes() {
    return RECIPIENT_TYPES;
  }

  @Override
  public void init(@Nonnull final NotificationSinkConfig cfg) {
    SlackConfig slackConfig = new SlackConfig();
    if (cfg.getStaticConfig().getOrDefault(PROXY_URL_CONFIG_NAME, null) != null) {
      slackConfig.setProxyUrl((String) cfg.getStaticConfig().get(PROXY_URL_CONFIG_NAME));
    }
    this.slack = Slack.getInstance(slackConfig);
    this.entityClient = cfg.getEntityClient();
    this.settingsProvider = cfg.getSettingsProvider();
    this.identityProvider = cfg.getIdentityProvider();
    this.entityNameProvider = cfg.getEntityNameProvider();
    this.secretProvider = cfg.getSecretProvider();
    this.baseUrl = cfg.getBaseUrl();
    this.connectionService = cfg.getConnectionService();
    // Optional -- Provide the bot token directly in config. Used until this is available inside UI.
    if (cfg.getStaticConfig().containsKey(BOT_TOKEN_CONFIG_NAME)) {
      staticConfigBotToken = (String) cfg.getStaticConfig().get(BOT_TOKEN_CONFIG_NAME);
    }
    // Optional -- Provide the default channel directly in config. Used until this is available
    // inside UI.
    if (cfg.getStaticConfig().containsKey(DEFAULT_CHANNEL_CONFIG_NAME)) {
      staticConfigDefaultChannel = (String) cfg.getStaticConfig().get(DEFAULT_CHANNEL_CONFIG_NAME);
    }
    // Optional - tell this sink that V2 sink is enabled, so to not support the V2 message types
    // during the migration.
    if (cfg.getStaticConfig().containsKey(SLACK_SINK_V2_ENABLED)) {
      slackSinkV2Enabled =
          Boolean.parseBoolean((String) cfg.getStaticConfig().get(SLACK_SINK_V2_ENABLED));
    }
    retryEnabled =
        Boolean.parseBoolean(
            (String) cfg.getStaticConfig().getOrDefault(RETRY_ENABLED_CONFIG_NAME, "true"));
    maxRetries =
        Integer.parseInt(
            (String) cfg.getStaticConfig().getOrDefault(MAX_NUM_RETRIES_CONFIG_NAME, "2"));
    retryAfterTimestamp = 0;
  }

  @Override
  public void send(
      @Nonnull OperationContext opContext,
      @Nonnull final NotificationRequest request,
      @Nonnull final NotificationContext context) {
    if (isEnabled(opContext)) {
      sendNotifications(opContext, request);
    } else {
      log.debug("Skipping sending notification for request {}. Slack sink not enabled.", request);
    }
  }

  /**
   * Returns true if slack notifications are enabled, false otherwise.
   *
   * <p>If Slack integration is ENABLED in Global Settings and a Slack Client can be instantiated,
   * then this method returns true.
   *
   * <p>Instantiation of the slack client simply depends on a "bot token" config being resolvable
   * from global settings or from static configuration.
   */
  @VisibleForTesting
  boolean isEnabled(@Nonnull OperationContext opContext) {

    final GlobalSettingsInfo globalSettings = this.settingsProvider.getGlobalSettings(opContext);

    if (globalSettings == null) {
      // Unable to resolve global settings. Cannot determine whether Slack should be enabled. Return
      // disabled.
      log.warn("Unable to resolve global settings. Slack is currently disabled.");
      return false;
    }

    // Next check global settings to determine whether slack is supposed to be enabled.
    if (globalSettings.getIntegrations().hasSlackSettings()) {

      // Slack should be enabled. Let's try to create a slack client (if one doesn't already exist)
      ObjectNode slackConnectionDetails = getSlackConnection(opContext);
      boolean didInitFromSlackConnectionDetails = false;
      if (slackConnectionDetails != null) {
        // First, let's try using the New Connection Entity.
        didInitFromSlackConnectionDetails = initSlackClientFromConnection(slackConnectionDetails);
      }

      if (!didInitFromSlackConnectionDetails) {
        // If no valid connection was found, fallback to using Global Settings
        initSlackClientFromGlobalSettings(globalSettings);
      }

      if (this.slackClient != null) {
        return true;
      } else {
        log.error(
            "Slack is enabled, but failed to create slack client! Missing required bot token.");
      }
    }
    // Slack is disabled in settings. Return false.
    return false;
  }

  private void sendNotifications(
      @Nonnull OperationContext opContext, final NotificationRequest notificationRequest) {
    // 1. Extract template type
    final NotificationTemplateType templateType =
        NotificationTemplateType.valueOf(notificationRequest.getMessage().getTemplate().toString());

    // 2. Send notifications
    final Set<ChatPostMessageResponse> responses;
    switch (templateType) {
      case CUSTOM:
        responses = sendCustomNotification(opContext, notificationRequest);
        break;
      case BROADCAST_NEW_INCIDENT:
        responses =
            sendBroadcastNotification(
                opContext,
                notificationRequest.getRecipients(),
                buildNewIncidentMessage(opContext, notificationRequest),
                RetryMode.ENABLED);
        break;
      case BROADCAST_INCIDENT_STATUS_CHANGE:
        responses =
            sendBroadcastNotification(
                opContext,
                notificationRequest.getRecipients(),
                buildIncidentStatusChangeMessage(opContext, notificationRequest),
                RetryMode.ENABLED);
        break;
      case BROADCAST_NEW_PROPOSAL:
        responses =
            sendBroadcastNotification(
                opContext,
                notificationRequest.getRecipients(),
                buildNewProposalMessage(opContext, notificationRequest),
                RetryMode.DISABLED);
        break;
      case BROADCAST_PROPOSAL_STATUS_CHANGE:
        responses =
            sendBroadcastNotification(
                opContext,
                notificationRequest.getRecipients(),
                buildProposalStatusChangeMessage(opContext, notificationRequest),
                RetryMode.DISABLED);
        break;
      case BROADCAST_ENTITY_CHANGE:
        responses =
            sendBroadcastNotification(
                opContext,
                notificationRequest.getRecipients(),
                buildEntityChangeMessage(opContext, notificationRequest),
                RetryMode.DISABLED);
        break;
      case BROADCAST_INGESTION_RUN_CHANGE:
        responses =
            sendBroadcastNotification(
                opContext,
                notificationRequest.getRecipients(),
                buildIngestionRunChangeMessage(notificationRequest),
                RetryMode.DISABLED);
        break;
      case BROADCAST_ASSERTION_STATUS_CHANGE:
        responses =
            sendBroadcastNotification(
                opContext,
                notificationRequest.getRecipients(),
                buildAssertionStatusChangeMessage(notificationRequest),
                RetryMode.ENABLED);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Unsupported template type %s providing to %s",
                templateType, this.getClass().getCanonicalName()));
    }

    // 3. Execute post notification actions
    // NOTE: we are moving all sinks to python, so slack_sink.py will need to replicate this
    // behavior
    // TODO: modify slack_sink.py to replicate this behavior
    executePostNotificationActions(opContext, notificationRequest, responses);
  }

  private void executePostNotificationActions(
      OperationContext opContext,
      NotificationRequest request,
      Set<ChatPostMessageResponse> responses) {
    // 1. Check parameters for flags that may justify post-notification actions
    StringMap parameters = request.getMessage().getParameters();
    if (parameters == null) {
      return;
    }

    // 2. Check for post send actions
    final String requestName = parameters.get("requestName");
    // 2.1 Emit ExecutionRequestResult aspects if this was triggered via a connection test request
    final String connectionTestRequestUrn =
        parameters.get(Constants.NOTIFICATION_CONNECTION_TEST_EXECUTION_REQUEST_URN_PARAM_KEY);
    if (requestName != null
        && requestName.equals(Constants.NOTIFICATION_CONNECTION_TEST_REQUEST_TEMPLATE_NAME)
        && connectionTestRequestUrn != null) {
      executePostSendActionForConnectionTest(opContext, responses, connectionTestRequestUrn);
    }
  }

  private void executePostSendActionForConnectionTest(
      final OperationContext opContext,
      final Set<ChatPostMessageResponse> responses,
      final String executionRequestUrnString) {
    try {
      // 1. Extract request urn
      final Urn requestUrn = UrnUtils.getUrn(executionRequestUrnString);

      // 2. Initialize the result aspect
      final ExecutionRequestResult result = new ExecutionRequestResult();

      // 3. Set the status
      final boolean anyError = responses.stream().anyMatch(o -> !o.isOk());
      result.setStatus(anyError ? "ERROR" : "SUCCESS");

      // 4. Build the reports for each slack response
      final List<Map<String, String>> responseReports =
          responses.stream()
              .map(
                  response -> {
                    Map<String, String> structuredReport = new HashMap<>();
                    structuredReport.put("error", response.getError());
                    structuredReport.put("warning", response.getWarning());
                    structuredReport.put("timestamp", response.getTs());
                    if (response.getMessage() != null) {
                      structuredReport.put(
                          "message",
                          response.getMessage() != null ? response.getMessage().toString() : null);
                    }
                    return structuredReport;
                  })
              .collect(Collectors.toList());

      // 5. Transform response reports into an overall StructuredExecutionReport
      final StructuredExecutionReport executionReport = new StructuredExecutionReport();
      executionReport.setType(Constants.NOTIFICATION_CONNECTION_TEST_EXECUTION_REPORT_TYPE);
      executionReport.setContentType("application/json");
      executionReport.setSerializedValue(JSON.serialize(responseReports));
      result.setStructuredReport(executionReport);

      // 6. Emit MCP with the final execution result aspect
      MetadataChangeProposal mcp =
          AspectUtils.buildMetadataChangeProposal(
              requestUrn, Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME, result);
      this.entityClient.ingestProposal(opContext, mcp, false);
    } catch (Exception e) {
      log.warn(
          String.format(
              "Encountered an exception while attempting to emit aspect for executionRequestResult, with requestUrn %s",
              executionRequestUrnString));
    }
  }

  private Set<ChatPostMessageResponse> sendCustomNotification(
      @Nonnull OperationContext opContext, final NotificationRequest request) {
    final String title = request.getMessage().getParameters().get("title");
    final String body = request.getMessage().getParameters().get("body");
    final String messageText = String.format("*%s*\n%s", title, body);
    return sendNotificationToRecipients(
        opContext, request.getRecipients(), messageText, RetryMode.DISABLED);
  }

  private String buildEntityChangeMessage(
      @Nonnull OperationContext opContext, NotificationRequest request) {
    final String actorName =
        getUserName(opContext, request.getMessage().getParameters().get("actorUrn"));
    final String entityName = request.getMessage().getParameters().get("entityName");
    final String entityUrl =
        String.format("%s%s", this.baseUrl, request.getMessage().getParameters().get("entityPath"));
    final String entityType = request.getMessage().getParameters().get("entityType");
    final String operation = request.getMessage().getParameters().get("operation");
    final String modifierType = request.getMessage().getParameters().get("modifierType");
    final String modifierStr =
        buildEntityChangeModifierString(request.getMessage().getParameters());

    if (DEPRECATION_MODIFIER_TYPE.equals(modifierType)) {
      return buildDeprecationMessage(actorName, entityName, entityUrl, entityType, operation);
    }

    // TODO: Handle Sub-resources (fields, etc)
    /*
     * Example:
     *     - John Joyce has added tag(s) PII, Test, Test2, + 3 more for SampleKafkaDataset.
     *     - John Joyce has updated deprecation for SampleKafkaDataset.
     *     - John Joyce has removed glossary term(s) a, b for SampleKafkaDataset.
     */
    return String.format(
        ">:pencil2:  *%s* has %s %s%s for %s *<%s|%s>*.",
        actorName, operation, modifierType, modifierStr, entityType, entityUrl, entityName);
  }

  private String buildDeprecationMessage(
      String actorName, String entityName, String entityUrl, String entityType, String operation) {
    // Deprecation following a slightly different structure:
    // Dataset SampleHiveDataset has been marked as deprecated by John Joyce.
    return String.format(
        ">:pencil2:  %s *<%s|%s>* has been %s by *%s*.",
        entityType, entityUrl, entityName, operation, actorName);
  }

  private String buildEntityChangeModifierString(Map<String, String> params) {
    // Handle multiple modifiers.
    final Integer modifierCount =
        params.get("modifierCount") != null ? Integer.valueOf(params.get("modifierCount")) : null;
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

  private String buildNewProposalMessage(
      @Nonnull OperationContext opContext, NotificationRequest request) {
    final String actorName =
        getUserName(opContext, request.getMessage().getParameters().get("actorUrn"));
    final String entityName = request.getMessage().getParameters().get("entityName");
    final String entityUrl =
        String.format("%s%s", this.baseUrl, request.getMessage().getParameters().get("entityPath"));
    final String modifierUrl =
        String.format(
            "%s%s", this.baseUrl, request.getMessage().getParameters().get("modifierPath"));
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
      return String.format(
          ":incoming_envelope: *New Proposal Raised*\n\n*%s* has proposed %s *<%s|%s>* for *%s* of %s *<%s|%s>*.",
          actorName,
          modifierType,
          modifierUrl,
          modifierName,
          maybeSubResource,
          entityType,
          entityUrl,
          entityName);
    }
    /*
     * Examples:
     *
     *     - John Joyce has proposed tag PII for SampleKafkaDataset.
     *     - John Joyce has proposed glossary term FOOBAR for SampleKafkaDataset.
     */
    return String.format(
        ":incoming_envelope: *New Proposal Raised*\n\n*%s* has proposed %s *<%s|%s>* for %s *<%s|%s>*.",
        actorName, modifierType, modifierUrl, modifierName, entityType, entityUrl, entityName);
  }

  private String buildProposalStatusChangeMessage(
      @Nonnull OperationContext opContext, NotificationRequest request) {
    // Fetch each user's email, this is required to understand their slack ids.
    final String actorName =
        getUserName(opContext, request.getMessage().getParameters().get("actorUrn"));
    final String entityName = request.getMessage().getParameters().get("entityName");
    final String entityUrl =
        String.format("%s%s", this.baseUrl, request.getMessage().getParameters().get("entityPath"));
    final String modifierUrl =
        String.format(
            "%s%s", this.baseUrl, request.getMessage().getParameters().get("modifierPath"));
    final String entityType = request.getMessage().getParameters().get("entityType");
    final String modifierType = request.getMessage().getParameters().get("modifierType");
    final String modifierName = request.getMessage().getParameters().get("modifierName");
    final String operation = request.getMessage().getParameters().get("operation");
    final String action = request.getMessage().getParameters().get("action");

    final String maybeSubResourceType = request.getMessage().getParameters().get("subResourceType");
    final String maybeSubResource = request.getMessage().getParameters().get("subResource");

    if (maybeSubResource != null && maybeSubResourceType != null) {
      return String.format(
          ":incoming_envelope: *Proposal Status Changed*\n\n*%s* has %s proposal to %s %s *<%s|%s>* for *%s* of %s *<%s|%s>*.",
          actorName,
          action,
          operation,
          modifierType,
          modifierUrl,
          modifierName,
          maybeSubResource,
          entityType,
          entityUrl,
          entityName);
    }
    return String.format(
        ":incoming_envelope: *Proposal Status Changed*\n\n*%s* has %s proposal to %s %s *<%s|%s>* for %s *<%s|%s>*.",
        actorName,
        action,
        operation,
        modifierType,
        modifierUrl,
        modifierName,
        entityType,
        entityUrl,
        entityName);
  }

  private String buildNewIncidentMessage(
      @Nonnull OperationContext opContext, NotificationRequest request) {

    // Extract owner urns, downstream owner urns.
    final List<Urn> ownerUrns =
        jsonToStrList(request.getMessage().getParameters().get("owners")).stream()
            .map(UrnUtils::getUrn)
            .collect(Collectors.toList());

    final List<Urn> downstreamOwnerUrns =
        jsonToStrList(request.getMessage().getParameters().get("downstreamOwners")).stream()
            .map(UrnUtils::getUrn)
            .collect(Collectors.toList());

    Map<Urn, IdentityProvider.User> usersMap =
        tryGetIncidentOwnerUserTagsAndNames(opContext, ownerUrns, downstreamOwnerUrns);
    Map<Urn, String> groupNames =
        tryGetIncidentOwnerGroupNames(opContext, ownerUrns, downstreamOwnerUrns);

    // Build the message.
    // TODO: Replace this with a template DSL (e.g. Jinja)
    final String url =
        String.format("%s%s", this.baseUrl, request.getMessage().getParameters().get("entityPath"));
    final String entityName = request.getMessage().getParameters().get("entityName");
    final String title = request.getMessage().getParameters().get("incidentTitle");
    final String description = request.getMessage().getParameters().get("incidentDescription");
    final String actorName =
        getUserName(opContext, request.getMessage().getParameters().get("actorUrn"));
    final String ownersStr = tryGetIncidentOwnersLabel(ownerUrns, usersMap, groupNames);
    final String downstreamOwnersStr =
        tryGetIncidentOwnersLabel(downstreamOwnerUrns, usersMap, groupNames);

    return String.format(
        "%s%s",
        String.format(
            ":warning: *New Incident Raised* \n\nA new incident has been raised on asset <%s|%s>%s.",
            url, entityName, actorName != null ? String.format(" by *%s*", actorName) : ""),
        String.format(
            "\n\n*Incident Name*: %s\n*Incident Description*: %s\n\n*Asset Owners*: %s\n*Downstream Asset Owners*: %s",
            title != null ? title : "None",
            description != null ? description : "None",
            ownersStr != null && !ownersStr.isEmpty() ? ownersStr : "None",
            downstreamOwnersStr != null && !downstreamOwnersStr.isEmpty()
                ? downstreamOwnersStr
                : "None"));
  }

  private String buildIncidentStatusChangeMessage(
      @Nonnull OperationContext opContext, NotificationRequest request) {

    // Extract owner urns, downstream owner urns.
    final List<Urn> ownerUrns =
        jsonToStrList(request.getMessage().getParameters().get("owners")).stream()
            .map(UrnUtils::getUrn)
            .collect(Collectors.toList());
    final List<Urn> downstreamOwnerUrns =
        jsonToStrList(request.getMessage().getParameters().get("downstreamOwners")).stream()
            .map(UrnUtils::getUrn)
            .collect(Collectors.toList());

    Map<Urn, IdentityProvider.User> usersMap =
        tryGetIncidentOwnerUserTagsAndNames(opContext, ownerUrns, downstreamOwnerUrns);
    Map<Urn, String> groupNames =
        tryGetIncidentOwnerGroupNames(opContext, ownerUrns, downstreamOwnerUrns);

    // Build the message. TODO: Use a template here.
    final String url =
        String.format("%s%s", this.baseUrl, request.getMessage().getParameters().get("entityPath"));
    final String entityName = request.getMessage().getParameters().get("entityName");
    final String message = request.getMessage().getParameters().get("message");
    final String title = request.getMessage().getParameters().get("incidentTitle");
    final String description = request.getMessage().getParameters().get("incidentDescription");
    final String prevStatus = request.getMessage().getParameters().get("prevStatus");
    final String newStatus = request.getMessage().getParameters().get("newStatus");
    final String actorName =
        getUserName(opContext, request.getMessage().getParameters().get("actorUrn"));
    final String ownersStr = tryGetIncidentOwnersLabel(ownerUrns, usersMap, groupNames);
    final String downstreamOwnersStr =
        tryGetIncidentOwnersLabel(downstreamOwnerUrns, usersMap, groupNames);

    final String icon = newStatus.equals("RESOLVED") ? ":white_check_mark:" : ":warning:";
    return String.format(
        "%s%s%s",
        String.format(
            "%s *Incident Status Changed*\n\nThe status of incident *%s* on asset <%s|%s> has changed from *%s* to *%s*%s.",
            icon,
            title != null ? title : "None",
            url,
            entityName,
            prevStatus,
            newStatus,
            actorName != null ? String.format(" by *%s*", actorName) : ""),
        String.format("\n\n*Message*: %s\n", message != null ? message : "None"),
        String.format(
            "\n\n*Incident Name*: %s\n*Incident Description*: %s\n\n*Asset Owners*: %s\n*Downstream Asset Owners*: %s",
            title != null ? title : "None",
            description != null ? description : "None",
            ownersStr != null && !ownersStr.isEmpty() ? ownersStr : "None",
            downstreamOwnersStr != null && !downstreamOwnersStr.isEmpty()
                ? downstreamOwnersStr
                : "None"));
  }

  private Map<Urn, IdentityProvider.User> tryGetIncidentOwnerUserTagsAndNames(
      @Nonnull OperationContext opContext, List<Urn> ownerUrns, List<Urn> downstreamOwnerUrns) {
    final Set<Urn> ownerUserUrns =
        ownerUrns.stream()
            .filter(urn -> urn.getEntityType().equals(CORP_USER_ENTITY_NAME))
            .collect(Collectors.toSet());

    final Set<Urn> downstreamOwnerUserUrns =
        downstreamOwnerUrns.stream()
            .filter(urn -> urn.getEntityType().equals(CORP_USER_ENTITY_NAME))
            .collect(Collectors.toSet());
    // Fetch each user's email, this is required to understand their slack ids, which lets us tag
    // them in the notif.
    final Set<Urn> allUsers = new HashSet<>();
    allUsers.addAll(ownerUserUrns);
    allUsers.addAll(downstreamOwnerUserUrns);
    Map<Urn, IdentityProvider.User> usersMap = Collections.emptyMap();
    try {
      usersMap = this.identityProvider.batchGetUsers(opContext, allUsers);
    } catch (Exception e) {
      // If we cannot resolve the users, still broadcast the message.
      log.warn("Failed to resolve users from GMS. Skipping tagging them in Slack broadcast.");
    }
    return usersMap;
  }

  private Map<Urn, String> tryGetIncidentOwnerGroupNames(
      @Nonnull OperationContext opContext, List<Urn> ownerUrns, List<Urn> downstreamOwnerUrns) {

    final Set<Urn> ownerGroupUrns =
        ownerUrns.stream()
            .filter(urn -> urn.getEntityType().equals(CORP_GROUP_ENTITY_NAME))
            .collect(Collectors.toSet());

    final Set<Urn> downstreamOwnerGroupUrns =
        downstreamOwnerUrns.stream()
            .filter(urn -> urn.getEntityType().equals(CORP_GROUP_ENTITY_NAME))
            .collect(Collectors.toSet());

    // Fetch each group's names.
    final Set<Urn> allGroups = new HashSet<>();
    allGroups.addAll(ownerGroupUrns);
    allGroups.addAll(downstreamOwnerGroupUrns);
    Map<Urn, String> groupNames = Collections.emptyMap();
    try {
      groupNames =
          this.entityNameProvider.batchGetName(opContext, allGroups, CORP_GROUP_ENTITY_NAME);
    } catch (Exception e) {
      // If we cannot resolve the users, still broadcast the message.
      log.warn("Failed to resolve groups from GMS. Skipping labeling them in Slack broadcast.");
    }
    return groupNames;
  }

  private String tryGetIncidentOwnersLabel(
      List<Urn> ownerUrns, Map<Urn, IdentityProvider.User> usersMap, Map<Urn, String> groupNames) {
    final Set<Urn> ownerUserUrns =
        ownerUrns.stream()
            .filter(e -> e.getEntityType().equals(CORP_USER_ENTITY_NAME))
            .collect(Collectors.toSet());
    final Set<Urn> ownerGroupUrns =
        ownerUrns.stream()
            .filter(e -> e.getEntityType().equals(CORP_GROUP_ENTITY_NAME))
            .collect(Collectors.toSet());

    final List<IdentityProvider.User> ownerUsers =
        usersMap.keySet().stream()
            .filter(ownerUserUrns::contains)
            .map(usersMap::get)
            .collect(Collectors.toList());
    final String ownersUsersStr = createUsersTagString(ownerUsers);
    final Set<String> ownersGroupNames =
        groupNames.keySet().stream()
            .filter(ownerGroupUrns::contains)
            .map(groupNames::get)
            .collect(Collectors.toSet());
    final String ownersGroupsStr = String.join(", ", ownersGroupNames);

    final int ownersLeftoverCount = ownerUrns.size() - ownerUsers.size() - ownersGroupNames.size();
    final String ownersLeftoversStr =
        ownersLeftoverCount > 0 ? String.format("+%s", ownersLeftoverCount) : null;
    return Stream.of(ownersUsersStr, ownersGroupsStr, ownersLeftoversStr)
        .filter(Objects::nonNull)
        .filter(str -> !str.isEmpty())
        .collect(Collectors.joining(", "));
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
    return String.format(
        ">:electric_plug:   Ingestion source *%s* of type *%s* has %s! <%s|View ingestion sources>.",
        sourceName, sourceType, statusText, ingestionUrl);
  }

  private String buildAssertionStatusChangeMessage(NotificationRequest request) {
    final String assertionUrn = request.getMessage().getParameters().get("assertionUrn");
    final String assertionType = request.getMessage().getParameters().get("assertionType");
    final String entityName = request.getMessage().getParameters().get("entityName");
    final String entityPath = request.getMessage().getParameters().get("entityPath");
    final String entityUrl = String.format("%s%s", this.baseUrl, entityPath);
    final String resultsUrl =
        String.format(
            "%s%s/Validation/Assertions?assertion_urn=%s",
            this.baseUrl, entityPath, urlEncode(assertionUrn));

    final String result = request.getMessage().getParameters().get("result");
    final String description = request.getMessage().getParameters().get("description");
    final String maybeExternalUrl =
        request.getMessage().getParameters().getOrDefault("externalUrl", null);
    final String maybeExternalPlatform =
        request.getMessage().getParameters().getOrDefault("externalPlatform", null);
    final String maybeSourceType =
        request.getMessage().getParameters().getOrDefault("sourceType", null);
    final String assertionTypeText =
        AssertionSourceType.INFERRED.toString().equals(maybeSourceType)
            ? "Smart Assertion"
            : String.format("%s Assertion", AssertionUtils.getAssertionTypeName(assertionType));
    final String resultString = AssertionUtils.getAssertionResultString(result);
    final String resultEmoji = AssertionUtils.getAssertionResultEmoji(result);
    final String resultsLink =
        maybeExternalUrl != null
            ? maybeExternalPlatform != null
                ? String.format("<%s|View results in %s>", maybeExternalUrl, maybeExternalPlatform)
                : String.format("<%s|View original results>", maybeExternalUrl)
            : String.format("<%s|View results>", resultsUrl);

    /*
     * Example: Assertion `column x must not be null` has failed for Dataset SampleHiveDataset! View results in dbt
     */
    return String.format(
        ">%s  *%s* `%s` has *%s* for *<%s|%s>*! %s",
        resultEmoji,
        assertionTypeText,
        description,
        resultString,
        entityUrl,
        entityName,
        resultsLink);
  }

  private Set<ChatPostMessageResponse> sendNotificationToRecipients(
      @Nonnull OperationContext opContext,
      final List<NotificationRecipient> recipients,
      final String text,
      RetryMode retryMode) {
    // Send each recipient a message.
    Set<ChatPostMessageResponse> responses = new HashSet<>();
    for (NotificationRecipient recipient : recipients) {
      ChatPostMessageResponse response =
          sendNotificationToRecipient(opContext, recipient, text, retryMode);
      if (response != null) {
        responses.add(response);
      } else {
        log.warn(
            String.format(
                "Notification for recipient with id %s did not result in a response. Post notification actions like emitting result MCPs will be skipped for this recipient.",
                recipient.getId()));
      }
    }
    return responses;
  }

  /**
   * Used by {@link #sendNotificationToRecipients(OperationContext, List, String, RetryMode)}. Call
   * that method directly instead of this.
   */
  private ChatPostMessageResponse sendNotificationToRecipient(
      @Nonnull OperationContext opContext,
      final NotificationRecipient recipient,
      final String text,
      RetryMode retryMode) {

    if (!isEligibleRecipientType(recipient)) {
      log.debug(
          "Skipping send notification to recipient. Unsupported recipient type {}",
          recipient.getType());
      return null;
    }

    // Try to sink message to each user.
    try {
      if (NotificationRecipientType.USER.equals(recipient.getType())) {
        return sendNotificationToUser(
            opContext, UrnUtils.getUrn(recipient.getId()), text, retryMode);
      } else if (NotificationRecipientType.SLACK_DM.equals(recipient.getType())) {
        if (!recipient.hasId() || recipient.getId() == null) {
          throw new UnsupportedOperationException(
              String.format("Tried to send a DM to user without ID set", recipient.getType()));
        }
        return sendMessage(recipient.getId(), text, retryMode);
      } else if (NotificationRecipientType.SLACK_CHANNEL.equals(recipient.getType())) {
        // We only support "SLACK_CHANNEL" as a custom type.
        String channel = getRecipientChannelOrDefault(opContext, recipient.getId(GetMode.NULL));
        if (channel != null) {
          return sendMessage(channel, text, retryMode);
        } else {
          log.warn(
              String.format(
                  "Failed to resolve channel for recipient of type %s. No default or provided channel.",
                  NotificationRecipientType.SLACK_CHANNEL));
        }
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "Failed to send Slack notification. Unsupported recipient type %s provided.",
                recipient.getType()));
      }
    } catch (Exception e) {
      log.error("Caught exception while attempting to send custom slack notification", e);
    }
    return null;
  }

  private ChatPostMessageResponse sendNotificationToUser(
      @Nonnull OperationContext opContext,
      final Urn userUrn,
      final String text,
      RetryMode retryMode)
      throws Exception {
    final IdentityProvider.User user =
        this.identityProvider.getUser(opContext, userUrn); // Retrieve DataHub User
    if (user != null && user.getEmail() != null) {
      User slackUser = getSlackUserFromEmail(user.getEmail());
      if (slackUser != null) {
        return sendMessage(slackUser.getId(), text, retryMode);
      }
    } else {
      log.warn(
          String.format(
              "Failed to send notification to user with urn %s. Failed to find user with valid email in DataHub.",
              userUrn));
    }
    return null;
  }

  private Set<ChatPostMessageResponse> sendBroadcastNotification(
      @Nonnull OperationContext opContext,
      final List<NotificationRecipient> recipients,
      final String text,
      RetryMode retryMode) {
    // In the case of a broadcast, if there are no recipients explicitly provided we fallback to
    // sending to the default configured channel.
    if (recipients.size() > 0) {
      // Send to each recipient in the list as normal.
      return sendNotificationToRecipients(opContext, recipients, text, retryMode);
    } else {
      // Broadcast to the default configured channel.
      NotificationRecipient defaultChannelRecipient =
          new NotificationRecipient().setType(NotificationRecipientType.SLACK_CHANNEL);
      return sendNotificationToRecipients(
          opContext, List.of(defaultChannelRecipient), text, retryMode);
    }
  }

  private ChatPostMessageResponse sendMessage(
      @Nonnull final String channel, @Nonnull final String text, RetryMode retryMode)
      throws Exception {
    final ChatPostMessageRequest msgRequest =
        ChatPostMessageRequest.builder()
            .channel(channel)
            .text(text)
            .iconUrl(String.format("%s%s", this.baseUrl, ACRYL_LOGO_FILE_PATH))
            .build();
    final ChatPostMessageResponse response = sendMessage(msgRequest, retryMode);
    // if response is null, the message has been dropped due to rate limiting. It has already been
    // logged.
    if (response != null) {
      if (response.isOk()) {
        log.debug(String.format("Successfully sent Slack notification to channel %s", channel));
      } else {
        log.error(
            "Failed to sink Slack notification to channel {} with text {}. Received error from Slack API: {}",
            channel,
            text,
            response.getError());
      }
    }
    return response;
  }

  @Nullable
  private ChatPostMessageResponse sendMessage(
      final ChatPostMessageRequest request, RetryMode retryMode) throws Exception {
    return sendMessage(request, 0, retryMode);
  }

  /*
   * Attempt to send a slack message if the current time is not before the timestamp we may have set to wait until if we have been
   * rate limited. Returns null if the message was dropped due to rate limiting.
   */
  @Nullable
  private ChatPostMessageResponse sendMessage(
      final ChatPostMessageRequest request, int retryAttempt, RetryMode retryMode)
      throws Exception {
    long currentTime = System.currentTimeMillis();
    if (currentTime < this.retryAfterTimestamp) {
      return optionallyRetrySendMessage(request, retryAttempt, retryMode, currentTime);
    }
    try {
      return slackClient.chatPostMessage(request);
    } catch (IOException | SlackApiException e) {
      if (e instanceof SlackApiException
          && ((SlackApiException) e).getResponse().code() == RATE_LIMIT_ERROR_CODE) {
        return handleRateLimitResponse((SlackApiException) e, request, retryAttempt, retryMode);
      } else {
        throw new Exception("Caught exception while attempting to send slack message", e);
      }
    }
  }

  private ChatPostMessageResponse handleRateLimitResponse(
      SlackApiException e,
      final ChatPostMessageRequest request,
      int retryAttempt,
      RetryMode retryMode)
      throws Exception {
    String retryAfter = e.getResponse().header(RETRY_AFTER_HEADER);
    log.info(
        String.format(
            "Reached Slack API rate limit. No new notifications will be sent for %s second(s)",
            retryAfter));
    if (retryAfter != null) {
      try {
        long currentTime = System.currentTimeMillis();
        int retryAfterInt = Integer.parseInt(retryAfter);
        this.retryAfterTimestamp = currentTime + retryAfterInt * 1000L;
        return optionallyRetrySendMessage(request, retryAttempt, retryMode, currentTime);
      } catch (NumberFormatException exc) {
        log.debug("Issue parsing retryAfter from slack API response headers", exc);
      }
    }
    MetricUtils.counter(this.getClass(), NOTIFICATION_DROPPED_METRIC).inc();
    return null;
  }

  private ChatPostMessageResponse optionallyRetrySendMessage(
      final ChatPostMessageRequest request, int retryAttempt, RetryMode retryMode, long currentTime)
      throws Exception {
    if (this.retryEnabled && retryMode.equals(RetryMode.ENABLED) && retryAttempt < maxRetries) {
      Thread.sleep(this.retryAfterTimestamp - currentTime);
      return sendMessage(request, retryAttempt + 1, retryMode);
    }
    log.debug(
        "Skipping sending notification for request {}. Pausing due to hitting our rate limit",
        request);
    MetricUtils.counter(this.getClass(), NOTIFICATION_DROPPED_METRIC).inc();
    return null;
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
   * Returns a formatted slack user tag string, e.g. <@JohnJoyce> if the user can be resolved to a
   * slack id, null if not.
   */
  @Nullable
  private String createUserTagString(final IdentityProvider.User user) {
    // Resolve a User object to their slack handle.
    if (user.getEmail() != null) {
      try {
        User slackUser = getSlackUserFromEmail(user.getEmail());
        // Add the slack user to the string.
        if (slackUser != null && slackUser.getId() != null) {
          return String.format("<@%s>", slackUser.getId());
        } else {
          log.warn(
              String.format(
                  "Skipping adding user with email %s to tag string. No corresponding slack user found.",
                  user.getEmail()));
        }
      } catch (Exception e) {
        log.error(
            String.format(
                "Caught exception while attempting to resolve user with email %s to slack user. Skipping adding user to tag string.",
                user.getEmail()),
            e);
      }
    } else {
      log.warn("Failed to resolve DataHub user to slack user by email. No email found for user!");
    }
    return String.format(
        "%s%s",
        user.getDisplayName(),
        user.getEmail() != null ? String.format("(%s)", user.getEmail()) : "");
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
        log.warn(
            String.format(
                "Received API error while attempting to resolve a Slack user with email %s. Error: %s",
                email, response.getError()));
      }
    }
    return null;
  }

  @Nullable
  private String getUserName(@Nonnull OperationContext opContext, final String userUrnStr) {
    try {
      if (userUrnStr.equals(SYSTEM_ACTOR)) {
        return SYSTEM_ACTOR_NAME;
      }
      Urn userUrn = Urn.createFromString(userUrnStr);
      IdentityProvider.User user = this.identityProvider.getUser(opContext, userUrn);
      return user != null ? user.getResolvedDisplayName() : null;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Invalid actor urn %s provided", userUrnStr));
    }
  }

  private UsersLookupByEmailResponse getSlackUserLookupResponseFromEmail(
      @Nonnull final String email) throws Exception {
    final UsersLookupByEmailRequest request =
        UsersLookupByEmailRequest.builder().email(email).build();
    try {
      return slackClient.usersLookupByEmail(request);
    } catch (IOException | SlackApiException e) {
      throw new Exception("Caught exception while attempting to lookup slack user by email", e);
    }
  }

  @Nullable
  private String getRecipientChannelOrDefault(
      @Nonnull OperationContext opContext, @Nullable final String recipientId) {
    return recipientId != null ? recipientId : getDefaultChannelName(opContext).orElse(null);
  }

  private Optional<String> getDefaultChannelName(@Nonnull OperationContext opContext) {
    // Resolves a fallback channel to send the notification to, in the case that a channel is not
    // provided.
    // Default channel provided in dynamic settings takes precedence over that provided in static
    // sink config.
    GlobalSettingsInfo globalSettings = this.settingsProvider.getGlobalSettings(opContext);
    return globalSettings != null
            && globalSettings.getIntegrations().hasSlackSettings()
            && globalSettings.getIntegrations().getSlackSettings().hasDefaultChannelName()
        ? Optional.ofNullable(
            globalSettings.getIntegrations().getSlackSettings().getDefaultChannelName())
        : Optional.ofNullable(this.staticConfigDefaultChannel);
  }

  private void initSlackClientFromGlobalSettings(@Nonnull final GlobalSettingsInfo globalSettings) {
    // Attempt to init the slack client from static config or local configuration.
    // Next, attempt to instantiate a slack client using a bot token from static config or
    // settings. Bot token provided in dynamic settings
    // takes precedence over that provided in static sink config.
    if (globalSettings.getIntegrations().hasSlackSettings()
        && globalSettings.getIntegrations().getSlackSettings().hasEncryptedBotToken()) {
      try {
        final String botToken =
            this.secretProvider.decryptSecret(
                globalSettings.getIntegrations().getSlackSettings().getEncryptedBotToken());
        createSlackClient(botToken);
      } catch (Exception e) {
        log.error(
            "Caught exception while attempting to resolve bot token secret. Failed to create slack client.",
            e);
      }
    } else if (this.staticConfigBotToken != null) {
      // Bot token provided in static configuration.
      createSlackClient(this.staticConfigBotToken);
    } else {
      log.warn(
          "Failed to create Slack client - could not resolve a bot token from static config or global settings!");
    }
  }

  private boolean initSlackClientFromConnection(@Nonnull final ObjectNode jsonConnection) {
    // Attempt to init the slack client from the connection object
    // Next, attempt to instantiate a slack client using a bot token from static config or
    // settings. Bot token provided in dynamic settings
    // takes precedence over that provided in static sink config.
    if (jsonConnection.has("bot_token")) {
      try {
        final String botToken = jsonConnection.get("bot_token").asText();
        createSlackClient(botToken);
        return true;
      } catch (Exception e) {
        log.error(
            "Caught exception while attempting to resolve bot token secret. Failed to create slack client.",
            e);
      }
    } else {
      log.warn(
          "Failed to create Slack client using Connection JSON! Falling back to legacy settings.");
    }
    return false;
  }

  private void createSlackClient(final String botToken) {
    if (hasChanged(botToken)) {
      log.info(
          "New slack bot token was found. Updating bot token and reinitializing slack client.");
      updateBotToken(botToken);
      this.slackClient = slack.methods(botToken);
    } else {
      log.debug("Bot token has not changed. Skipping reinitialization of slack client.");
    }
  }

  @Nullable
  private ObjectNode getSlackConnection(@Nonnull OperationContext opContext) {
    final DataHubConnectionDetails details =
        this.connectionService.getConnectionDetails(opContext, SLACK_CONNECTION_URN);
    if (details != null
        && DataHubConnectionDetailsType.JSON.equals(details.getType())
        && details.hasJson()) {
      try {
        final String jsonStr =
            this.secretProvider.decryptSecret(details.getJson().getEncryptedBlob());
        return new ObjectMapper().readValue(jsonStr, ObjectNode.class);
      } catch (Exception e) {
        log.error(
            String.format(
                "Failed to decode Slack Connection details from connection %s. Falling back to global settings.",
                details));
      }
    }
    return null;
  }

  private String urlEncode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Failed to encode string", e);
    }
  }

  /**
   * Returns true if the recipient is of an eligible type to receive a slack notification, false
   * otherwise which means the recipient cannot be handled by this sink.
   */
  private boolean isEligibleRecipientType(NotificationRecipient recipient) {
    return NotificationRecipientType.USER.equals(recipient.getType())
        || NotificationRecipientType.SLACK_DM.equals(recipient.getType())
        || NotificationRecipientType.SLACK_CHANNEL.equals(recipient.getType());
  }

  private synchronized boolean hasChanged(final String newBotToken) {
    return !newBotToken.equals(this.botToken);
  }

  public synchronized void updateBotToken(final String newBotToken) {
    this.botToken = newBotToken;
  }
}
