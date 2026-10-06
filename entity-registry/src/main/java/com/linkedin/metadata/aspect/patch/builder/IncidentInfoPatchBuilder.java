package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.INCIDENT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INCIDENT_INFO_ASPECT_NAME;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentStage;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.mxe.GenericAspect;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

/** Builds JSON Patch proposals for the editable fields of an IncidentInfo aspect. */
public class IncidentInfoPatchBuilder
    extends AbstractMultiFieldPatchBuilder<IncidentInfoPatchBuilder> {

  // NON_NULL: "remove" operations carry no value (RFC 6902); a bare Jackson mapper would
  // otherwise serialize the Java-null value field as an explicit "value": null.
  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  private static final String PATH_DELIM = "/";
  private static final String TITLE_FIELD = "title";
  private static final String DESCRIPTION_FIELD = "description";
  private static final String STARTED_AT_FIELD = "startedAt";
  private static final String STATUS_FIELD = "status";
  private static final String STATUS_STATE_FIELD = "status/state";
  private static final String STATUS_STAGE_FIELD = "status/stage";
  private static final String STATUS_MESSAGE_FIELD = "status/message";
  private static final String STATUS_LAST_UPDATED_FIELD = "status/lastUpdated";
  private static final String PRIORITY_FIELD = "priority";
  private static final String ENTITIES_FIELD = "entities";
  private static final String ASSIGNEES_FIELD = "assignees";

  public IncidentInfoPatchBuilder setTitle(@Nonnull String title) {
    addValue(TITLE_FIELD, instance.textNode(title));
    return this;
  }

  public IncidentInfoPatchBuilder clearTitle() {
    removeValue(TITLE_FIELD);
    return this;
  }

  public IncidentInfoPatchBuilder setDescription(@Nonnull String description) {
    addValue(DESCRIPTION_FIELD, instance.textNode(description));
    return this;
  }

  public IncidentInfoPatchBuilder clearDescription() {
    removeValue(DESCRIPTION_FIELD);
    return this;
  }

  /** updateIncident only: startedAt is not editor-owned, so upsertIncident never sets this. */
  public IncidentInfoPatchBuilder setStartedAt(@Nonnull Long startedAt) {
    addValue(STARTED_AT_FIELD, instance.numberNode(startedAt));
    return this;
  }

  /** Replaces the entire status object. Used by the editor's full-snapshot upsert. */
  public IncidentInfoPatchBuilder setStatus(@Nonnull IncidentStatus status) {
    addRecord(STATUS_FIELD, status);
    return this;
  }

  /** Patches only the status state, leaving stage and message untouched. */
  public IncidentInfoPatchBuilder setStatusState(@Nonnull IncidentState state) {
    addValue(STATUS_STATE_FIELD, instance.textNode(state.toString()));
    return this;
  }

  /** Patches only the status stage, leaving state and message untouched. */
  public IncidentInfoPatchBuilder setStatusStage(@Nonnull IncidentStage stage) {
    addValue(STATUS_STAGE_FIELD, instance.textNode(stage.toString()));
    return this;
  }

  /** Patches only the status message, leaving state and stage untouched. */
  public IncidentInfoPatchBuilder setStatusMessage(@Nonnull String message) {
    addValue(STATUS_MESSAGE_FIELD, instance.textNode(message));
    return this;
  }

  /** Patches only the status lastUpdated audit stamp, leaving state/stage/message untouched. */
  public IncidentInfoPatchBuilder setStatusLastUpdated(@Nonnull AuditStamp lastUpdated) {
    addRecord(STATUS_LAST_UPDATED_FIELD, lastUpdated);
    return this;
  }

  public IncidentInfoPatchBuilder setPriority(@Nonnull Integer priority) {
    addValue(PRIORITY_FIELD, instance.numberNode(priority));
    return this;
  }

  public IncidentInfoPatchBuilder clearPriority() {
    removeValue(PRIORITY_FIELD);
    return this;
  }

  public IncidentInfoPatchBuilder setEntities(@Nonnull List<Urn> entities) {
    ArrayNode entityArray = instance.arrayNode();
    entities.forEach(entity -> entityArray.add(entity.toString()));
    addValue(ENTITIES_FIELD, entityArray);
    return this;
  }

  public IncidentInfoPatchBuilder setAssignees(@Nonnull IncidentAssigneeArray assignees) {
    try {
      IncidentInfo wrapper = new IncidentInfo().setAssignees(assignees);
      addValue(
          ASSIGNEES_FIELD,
          OBJECT_MAPPER.readTree(RecordUtils.toJsonString(wrapper)).get(ASSIGNEES_FIELD));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize incident assignees.", e);
    }
    return this;
  }

  private void addValue(String field, com.fasterxml.jackson.databind.JsonNode value) {
    pathValues.add(
        ImmutableTriple.of(PatchOperationType.ADD.getValue(), PATH_DELIM + field, value));
  }

  private void removeValue(String field) {
    pathValues.add(
        ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), PATH_DELIM + field, null));
  }

  private void addRecord(String field, com.linkedin.data.template.RecordTemplate record) {
    try {
      addValue(field, OBJECT_MAPPER.readTree(RecordUtils.toJsonString(record)));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Failed to serialize incident patch field " + field + ".", e);
    }
  }

  @Override
  protected String getAspectName() {
    return INCIDENT_INFO_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return INCIDENT_ENTITY_NAME;
  }

  /**
   * Emits the modern {@link GenericJsonPatch} envelope with {@code forceGenericPatch: true}. The
   * inherited default emits a bare JSON Patch array, which routes through the legacy per-aspect
   * Template engine at apply time; {@code incidentInfo} has no registered Template, so that path
   * fails at GMS even though unit tests inspecting only the serialized operations would pass.
   */
  @Override
  protected GenericAspect buildPatch() {
    if (pathValues.isEmpty()) {
      throw new IllegalArgumentException("No patches specified.");
    }

    List<GenericJsonPatch.PatchOp> patchOps =
        pathValues.stream()
            .map(
                triple -> {
                  GenericJsonPatch.PatchOp op = new GenericJsonPatch.PatchOp();
                  op.setOp(triple.left);
                  op.setPath(triple.middle);
                  if (triple.right != null) {
                    op.setValue(OBJECT_MAPPER.convertValue(triple.right, Object.class));
                  }
                  return op;
                })
            .collect(Collectors.toList());

    GenericJsonPatch genericJsonPatch =
        GenericJsonPatch.builder().patch(patchOps).forceGenericPatch(true).build();

    // Mirrors GenericRecordUtils.serializePatch(GenericJsonPatch, ObjectMapper) inline: this
    // module cannot depend on metadata-utils, which itself depends on entity-registry.
    try {
      GenericAspect genericAspect = new GenericAspect();
      genericAspect.setValue(
          ByteString.copyString(
              OBJECT_MAPPER.writeValueAsString(genericJsonPatch), StandardCharsets.UTF_8));
      genericAspect.setContentType("application/json-patch+json");
      return genericAspect;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize incident patch envelope.", e);
    }
  }
}
