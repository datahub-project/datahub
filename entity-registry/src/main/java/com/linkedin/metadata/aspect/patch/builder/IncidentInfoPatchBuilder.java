package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.INCIDENT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INCIDENT_INFO_ASPECT_NAME;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

/** Builds JSON Patch proposals for the editable fields of an IncidentInfo aspect. */
public class IncidentInfoPatchBuilder
    extends AbstractMultiFieldPatchBuilder<IncidentInfoPatchBuilder> {

  private static final String PATH_DELIM = "/";
  private static final String TITLE_FIELD = "title";
  private static final String DESCRIPTION_FIELD = "description";
  private static final String STARTED_AT_FIELD = "startedAt";
  private static final String STATUS_FIELD = "status";
  private static final String PRIORITY_FIELD = "priority";
  private static final String ENTITIES_FIELD = "entities";
  private static final String ASSIGNEES_FIELD = "assignees";

  public IncidentInfoPatchBuilder setTitle(@Nonnull String title) {
    addValue(TITLE_FIELD, instance.textNode(title));
    return this;
  }

  public IncidentInfoPatchBuilder setDescription(@Nonnull String description) {
    addValue(DESCRIPTION_FIELD, instance.textNode(description));
    return this;
  }

  public IncidentInfoPatchBuilder setStartedAt(@Nonnull Long startedAt) {
    addValue(STARTED_AT_FIELD, instance.numberNode(startedAt));
    return this;
  }

  public IncidentInfoPatchBuilder setStatus(@Nonnull IncidentStatus status) {
    addRecord(STATUS_FIELD, status);
    return this;
  }

  public IncidentInfoPatchBuilder setPriority(@Nonnull Integer priority) {
    addValue(PRIORITY_FIELD, instance.numberNode(priority));
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
          new ObjectMapper().readTree(RecordUtils.toJsonString(wrapper)).get(ASSIGNEES_FIELD));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize incident assignees.", e);
    }
    return this;
  }

  private void addValue(String field, com.fasterxml.jackson.databind.JsonNode value) {
    pathValues.add(
        ImmutableTriple.of(PatchOperationType.ADD.getValue(), PATH_DELIM + field, value));
  }

  private void addRecord(String field, com.linkedin.data.template.RecordTemplate record) {
    try {
      addValue(field, new ObjectMapper().readTree(RecordUtils.toJsonString(record)));
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
}
