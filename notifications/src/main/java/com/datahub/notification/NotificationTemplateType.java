package com.datahub.notification;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Set;

/**
 * The list of all supported notification templates, along with their required + optional inputs /
 * outputs. Notification templates represent the content required to render a notification of a
 * given type.
 *
 * <p>Notification template types do not necessarily represent the semantic type of a notification,
 * and can be shared across notification varying recipient types and sinks.
 */
public enum NotificationTemplateType {
  /** A "custom" notification, e.g. one that has a simply title & text. */
  CUSTOM(ImmutableSet.of("title", "body"), Collections.emptySet()),
  /** Broadcast that a incident was created to a particular channel. */
  BROADCAST_NEW_INCIDENT(
      ImmutableSet.of("incidentUrn", "entityUrn", "entityPath", "newStatus"),
      ImmutableSet.of(
          "incidentTitle",
          "incidentDescription",
          "actorUrn",
          "actorName",
          "entityOwners",
          "downstreamEntityOwners")),
  /** Broadcast that a new incident that was previously broadcasted was updated. */
  BROADCAST_NEW_INCIDENT_UPDATE(
      ImmutableSet.of("incidentUrn", "entityUrn", "entityPath", "newStatus"),
      ImmutableSet.of(
          "incidentTitle",
          "incidentDescription",
          "actorUrn",
          "actorName",
          "entityOwners",
          "downstreamEntityOwners")),
  /** Broadcast that a incident's status has changed. */
  BROADCAST_INCIDENT_STATUS_CHANGE(
      ImmutableSet.of("incidentUrn", "entityUrn", "entityPath", "newStatus"),
      ImmutableSet.of(
          "message",
          "incidentTitle",
          "incidentDescription",
          "actorUrn",
          "actorName",
          "entityOwners",
          "downstreamEntityOwners")),
  /**
   * Broadcast that an entity has changed: owners added or removed, tags, terms, domain, deprecation
   */
  BROADCAST_ENTITY_CHANGE(
      ImmutableSet.of(
          "entityName", "entityPath", "entityType", "operation", "actorUrn", "actorName"),
      ImmutableSet.of(
          "modifierType",
          "modifierCount",
          "modifier0Name",
          "modifier0Path",
          "modifier1Name",
          "modifier1Path",
          "modifier2Name",
          "modifier2Path",
          "subResource",
          "subResourceType",
          // Deprecation Optional Parameters.
          "timestamp",
          "note")),
  /** Broadcast that an managed ingestion run has changed. */
  BROADCAST_INGESTION_RUN_CHANGE(
      ImmutableSet.of("sourceName", "sourceType", "statusText"),
      ImmutableSet.of("ingestionSourceUrn", "executionRequestUrn")),
  /** Broadcast a change proposal: tag, term, ownership, documentation, domain proposal. */
  BROADCAST_NEW_PROPOSAL(
      ImmutableSet.of(
          "operation",
          "modifierType",
          "modifierNames",
          "modifierPaths",
          "entityName",
          "entityType",
          "actorUrn",
          "actorName"),
      ImmutableSet.of("entityPath", "subResourceType", "subResourceUrn")),
  /** Broadcast a change proposal update, eg an APPROVE or DENY. */
  BROADCAST_PROPOSAL_STATUS_CHANGE(
      ImmutableSet.of(
          "operation",
          "modifierType",
          "modifierNames",
          "modifierPaths",
          "entityName",
          "entityType",
          "action",
          "actorUrn",
          "actorName"),
      ImmutableSet.of(
              "entityPath",
              "subResourceType",
              "subResourceUrn")),
  /** Broadcast an Assertion status change as a FAILURE or SUCCESS */
  BROADCAST_ASSERTION_STATUS_CHANGE(
      ImmutableSet.of("assertionUrn", "entityName", "entityPath", "result"),
      Collections.emptySet());

  private final Set<String> requiredParameters;
  private final Set<String> optionalParameters;

  NotificationTemplateType(final Set<String> requiredFields, final Set<String> optionalFields) {
    this.requiredParameters = requiredFields;
    this.optionalParameters = optionalFields;
  }

  public Set<String> getRequiredParameters() {
    return this.requiredParameters;
  }

  public Set<String> getOptionalFields() {
    return this.optionalParameters;
  }
}
