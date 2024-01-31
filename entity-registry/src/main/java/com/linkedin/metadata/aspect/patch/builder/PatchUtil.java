package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.UNKNOWN_ACTOR;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;

public class PatchUtil {
  private PatchUtil() {}

  private static final String TIME_KEY = "time";
  private static final String ACTOR_KEY = "actor";
  private static final String IMPERSONATOR_KEY = "impersonator";
  private static final String MESSAGE_KEY = "message";
  private static final String LAST_MODIFIED_KEY = "lastModified";
  private static final String CREATED_KEY = "created";
  private static final String DESTINATION_URN_KEY = "destinationUrn";
  private static final String SOURCE_URN_KEY = "sourceUrn";

  private static final String PROPERTIES_KEY = "properties";

  public static ObjectNode createEdgeValue(@Nonnull Edge edge) {
    ObjectNode value = instance.objectNode();

    ObjectNode created = instance.objectNode();
    if (edge.getCreated() == null) {
      created.put(TIME_KEY, System.currentTimeMillis()).put(ACTOR_KEY, UNKNOWN_ACTOR);
    } else {
      created
          .put(TIME_KEY, edge.getCreated().getTime())
          .put(ACTOR_KEY, edge.getCreated().getActor().toString());
      if (edge.getCreated().getImpersonator() != null) {
        created.put(IMPERSONATOR_KEY, edge.getCreated().getImpersonator().toString());
      }
      if (edge.getCreated().getMessage() != null) {
        created.put(MESSAGE_KEY, edge.getCreated().getMessage());
      }
    }
    value.set(CREATED_KEY, created);

    ObjectNode lastModified = instance.objectNode();
    if (edge.getLastModified() == null) {
      lastModified.put(TIME_KEY, System.currentTimeMillis()).put(ACTOR_KEY, UNKNOWN_ACTOR);
    } else {
      lastModified
          .put(TIME_KEY, edge.getLastModified().getTime())
          .put(ACTOR_KEY, edge.getLastModified().getActor().toString());
      if (edge.getLastModified().getImpersonator() != null) {
        lastModified.put(IMPERSONATOR_KEY, edge.getLastModified().getImpersonator().toString());
      }
      if (edge.getLastModified().getMessage() != null) {
        lastModified.put(MESSAGE_KEY, edge.getLastModified().getMessage());
      }
    }
    value.set(LAST_MODIFIED_KEY, lastModified);

    if (edge.getProperties() != null) {
      ObjectNode propertiesNode = instance.objectNode();
      edge.getProperties().forEach((k, v) -> propertiesNode.set(k, instance.textNode(v)));
      value.set(PROPERTIES_KEY, propertiesNode);
    }

    value.put(DESTINATION_URN_KEY, edge.getDestinationUrn().toString());
    if (edge.getSourceUrn() != null) {
      value.put(SOURCE_URN_KEY, edge.getSourceUrn().toString());
    }

    return value;
  }

  public static ObjectNode createEdgeValue(@Nonnull Urn urn) {
    ObjectNode value = instance.objectNode();
    ObjectNode auditStamp = instance.objectNode();
    auditStamp.put(TIME_KEY, System.currentTimeMillis()).put(ACTOR_KEY, UNKNOWN_ACTOR);

    value.put(DESTINATION_URN_KEY, urn.toString()).set(LAST_MODIFIED_KEY, auditStamp);
    value.set(CREATED_KEY, auditStamp);

    return value;
  }
}
