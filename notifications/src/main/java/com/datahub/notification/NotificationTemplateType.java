package com.datahub.notification;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Set;


/**
 * The list of all supported notification templates, along with their required + optional inputs / outputs.
 * Notification templates represent the content required to render a notification of a given type.
 *
 * Notification template types do not necessarily represent the semantic type
 * of a notification, and can be shared across notification varying recipient types and sinks.
 */
public enum NotificationTemplateType {
  /**
   * A "custom" notification, e.g. one that has a simply title & text.
   */
  CUSTOM(
      ImmutableSet.of(
          "title",
          "body"
      ),
      Collections.emptySet()
  ),
  /**
   * Broadcast that a incident was created to a particular channel.
   */
  BROADCAST_NEW_INCIDENT(
      ImmutableSet.of(
          "incidentUrn",
          "entityUrn",
          "entityPath",
          "newStatus"
      ),
      ImmutableSet.of(
          "incidentTitle",
          "incidentDescription",
          "actorUrn",
          "entityOwners",
          "downstreamEntityOwners"
      )
  ),
  /**
   * Broadcast that a incident's status has changed.
   */
  BROADCAST_INCIDENT_STATUS_CHANGE(
      ImmutableSet.of(
          "incidentUrn",
          "entityUrn",
          "entityPath",
          "newStatus"
      ),
      ImmutableSet.of(
          "incidentTitle",
          "incidentDescription",
          "actorUrn",
          "entityOwners",
          "downstreamEntityOwners"
      )
  ),;

  private final Set<String> requiredParameters;
  private final Set<String> optionalParameters;

  NotificationTemplateType(
      final Set<String> requiredFields,
      final Set<String> optionalFields
  ) {
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
