/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.telemetry;

public class OpenTelemetryKeyConstants {
  private OpenTelemetryKeyConstants() {}

  // OTEL Attributes
  public static final String USER_ID_ATTR = "user.id";
  public static final String REQUEST_API_ATTR = "request.api";
  public static final String REQUEST_ID_ATTR = "request.id";
  public static final String ACTOR_URN_ATTR =
      "actor.urn"; // TODO: Evaluate if this duplication of user.id is needed
  public static final String EVENT_TYPE_ATTR = "event.type";
  public static final String USER_AGENT_ATTR = "user.agent";
  public static final String ENTITY_URN_ATTR = "entity.urn";
  public static final String ENTITY_TYPE_ATTR = "entity.type";
  public static final String ASPECT_NAME_ATTR = "aspect.name";
  public static final String LOGIN_SOURCE_ATTR = "login.source";
  public static final String TELEMETRY_TRACE_ID_ATTR = "telemetry.trace.id";

  // OTEL Event Source Context Mutable Baggage
  public static final String EVENT_SOURCE = "event.source";
  public static final String SOURCE_IP = "source.ip";

  // OTEL Span Event types
  public static final String LOGIN_EVENT = "event.login";
  public static final String UPDATE_ASPECT_EVENT = "event.update.aspect";
}
