package com.linkedin.metadata.timeline.eventgenerator;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataprocess.DataProcessInstanceRelationships;
import com.linkedin.dataprocess.DataProcessInstanceRunEvent;
import com.linkedin.dataprocess.DataProcessRunStatus;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.Constants.*;


public class DataProcessInstanceRunEventChangeEventGenerator
    extends EntityChangeEventGenerator<DataProcessInstanceRunEvent> {
  private static final String COMPLETED_STATUS = "COMPLETED";
  private static final String STARTED_STATUS = "STARTED";

  public DataProcessInstanceRunEventChangeEventGenerator(@Nonnull final EntityClient entityClient, @Nonnull final
  Authentication authentication) {
    super(entityClient, authentication);
  }

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<DataProcessInstanceRunEvent> from,
      @Nonnull Aspect<DataProcessInstanceRunEvent> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }

  private List<ChangeEvent> computeDiffs(
      final DataProcessInstanceRunEvent previousAspect,
      final DataProcessInstanceRunEvent newAspect,
      @Nonnull final String entityUrn,
      @Nonnull final AuditStamp auditStamp) {
    final DataProcessRunStatus previousStatus = getStatus(previousAspect);
    final DataProcessRunStatus newStatus = getStatus(newAspect);

    if (newStatus != null && !newStatus.equals(previousStatus)) {
      String operationType = newStatus.equals(DataProcessRunStatus.COMPLETE) ? COMPLETED_STATUS : STARTED_STATUS;

      return Collections.singletonList(ChangeEvent.builder()
          .category(ChangeCategory.RUN)
          .operation(ChangeOperation.valueOf(operationType))
          .auditStamp(auditStamp)
          .entityUrn(entityUrn)
          .parameters(buildParameters(newAspect, entityUrn))
          .build());
    }

    return Collections.emptyList();
  }

  @Nullable
  private DataProcessRunStatus getStatus(DataProcessInstanceRunEvent dataProcessInstanceRunEvent) {
    return dataProcessInstanceRunEvent != null ? dataProcessInstanceRunEvent.getStatus() : null;
  }

  @Nonnull
  private Map<String, Object> buildParameters(@Nonnull final DataProcessInstanceRunEvent runEvent,
      @Nonnull final String entityUrnString) {
    final Map<String, Object> parameters = new HashMap<>();
    if (runEvent.hasAttempt()) {
      parameters.put(ATTEMPT_KEY, runEvent.getAttempt());
    }
    if (runEvent.hasResult()) {
      parameters.put(RUN_RESULT_KEY, runEvent.getResult().getType().toString());
    }

    DataProcessInstanceRelationships relationships = getRelationships(entityUrnString);

    if (relationships.hasParentInstance()) {
      parameters.put(PARENT_INSTANCE_URN_KEY, relationships.getParentInstance().toString());
    }

    if (relationships.hasParentTemplate()) {
      Urn parentTemplateUrn = relationships.getParentTemplate();
      if (parentTemplateUrn.getEntityType().equals(DATA_FLOW_ENTITY_NAME)) {
        parameters.put(DATA_FLOW_URN_KEY, parentTemplateUrn.toString());
      } else if (parentTemplateUrn.getEntityType().equals(DATA_JOB_ENTITY_NAME)) {
        parameters.put(DATA_JOB_URN_KEY, parentTemplateUrn.toString());
      }
    }
    return parameters;
  }

  @Nullable
  private DataProcessInstanceRelationships getRelationships(@Nonnull final String entityUrnString) {
    Urn entityUrn;
    EntityResponse entityResponse;
    try {
      entityUrn = Urn.createFromString(entityUrnString);
      entityResponse = _entityClient.getV2(DATA_PROCESS_INSTANCE_ENTITY_NAME, entityUrn,
          Collections.singleton(DATA_PROCESS_INSTANCE_RELATIONSHIPS_ASPECT_NAME), _authentication);
    } catch (Exception e) {
      return null;
    }

    if (entityResponse == null) {
      return null;
    }

    final EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    // If invite token aspect is not present, create a new one. Otherwise, return existing one.
    if (!aspectMap.containsKey(DATA_PROCESS_INSTANCE_RELATIONSHIPS_ASPECT_NAME)) {
      return null;
    }

    return new DataProcessInstanceRelationships(
        aspectMap.get(DATA_PROCESS_INSTANCE_RELATIONSHIPS_ASPECT_NAME).getValue().data());
  }
}
