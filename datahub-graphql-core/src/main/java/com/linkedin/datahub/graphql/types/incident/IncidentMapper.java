package com.linkedin.datahub.graphql.types.incident;

import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Incident;
import com.linkedin.datahub.graphql.generated.IncidentPriority;
import com.linkedin.datahub.graphql.generated.IncidentSource;
import com.linkedin.datahub.graphql.generated.IncidentSourceType;
import com.linkedin.datahub.graphql.generated.IncidentStage;
import com.linkedin.datahub.graphql.generated.IncidentState;
import com.linkedin.datahub.graphql.generated.IncidentStatus;
import com.linkedin.datahub.graphql.generated.IncidentType;
import com.linkedin.datahub.graphql.generated.OwnerType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.incident.IncidentAssignee;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.metadata.Constants;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Maps a GMS {@link EntityResponse} to a GraphQL incident. */
@Slf4j
public class IncidentMapper {

  public static Incident map(@Nullable QueryContext context, final EntityResponse entityResponse) {
    final Incident result = new Incident();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();
    result.setType(EntityType.INCIDENT);
    result.setUrn(entityUrn.toString());

    final EnvelopedAspect envelopedIncidentInfo = aspects.get(Constants.INCIDENT_INFO_ASPECT_NAME);
    if (envelopedIncidentInfo != null) {
      final IncidentInfo info = new IncidentInfo(envelopedIncidentInfo.getValue().data());
      // Assumption alert! This assumes the incident type in GMS exactly equals that in GraphQL
      result.setIncidentType(IncidentType.valueOf(info.getType().name()));
      result.setCustomType(info.getCustomType(GetMode.NULL));
      result.setTitle(info.getTitle(GetMode.NULL));
      result.setDescription(info.getDescription(GetMode.NULL));
      result.setPriority(mapPriority(info.getPriority(GetMode.NULL)));
      result.setAssignees(mapAssignees(info.getAssignees(GetMode.NULL)));
      // TODO: Support multiple entities per incident.
      result.setEntity(UrnToEntityMapper.map(context, info.getEntities().get(0)));
      if (info.hasStartedAt()) {
        result.setStartedAt(info.getStartedAt());
      }
      if (info.hasSource()) {
        result.setSource(mapIncidentSource(context, info.getSource()));
      }
      if (info.hasStatus()) {
        result.setStatus(mapStatus(context, info.getStatus()));
      }
      result.setCreated(AuditStampMapper.map(context, info.getCreated()));
    } else {
      throw new RuntimeException(String.format("Incident does not exist!. urn: %s", entityUrn));
    }

    final EnvelopedAspect envelopedTags = aspects.get(GLOBAL_TAGS_ASPECT_NAME);
    if (envelopedTags != null) {
      result.setTags(
          GlobalTagsMapper.map(
              context, new GlobalTags(envelopedTags.getValue().data()), entityUrn));
    }

    return result;
  }

  private static IncidentStatus mapStatus(
      @Nullable QueryContext context, final com.linkedin.incident.IncidentStatus incidentStatus) {
    final IncidentStatus result = new IncidentStatus();
    result.setState(IncidentState.valueOf(incidentStatus.getState().name()));
    result.setMessage(incidentStatus.getMessage(GetMode.NULL));
    result.setLastUpdated(AuditStampMapper.map(context, incidentStatus.getLastUpdated()));
    if (incidentStatus.hasStage()) {
      result.setStage(IncidentStage.valueOf(incidentStatus.getStage().toString()));
    }
    return result;
  }

  @Nullable
  private static IncidentPriority mapPriority(@Nullable final Integer priority) {
    if (priority == null) {
      return null;
    }
    switch (priority) {
      case 3:
        return IncidentPriority.LOW;
      case 2:
        return IncidentPriority.MEDIUM;
      case 1:
        return IncidentPriority.HIGH;
      case 0:
        return IncidentPriority.CRITICAL;
      default:
        log.error(String.format("Invalid priority value: %s", priority));
        return null;
    }
  }

  @Nullable
  private static List<OwnerType> mapAssignees(@Nullable final List<IncidentAssignee> assignees) {
    if (assignees == null) {
      return null;
    }
    return assignees.stream().map(IncidentMapper::mapAssignee).collect(Collectors.toList());
  }

  private static OwnerType mapAssignee(final IncidentAssignee assignee) {
    Urn actor = assignee.getActor();
    if (actor.getEntityType().equals(Constants.CORP_USER_ENTITY_NAME)) {
      final CorpUser user = new CorpUser();
      user.setUrn(actor.toString());
      user.setType(EntityType.CORP_USER);
      return user;
    } else if (actor.getEntityType().equals(Constants.CORP_GROUP_ENTITY_NAME)) {
      final CorpGroup group = new CorpGroup();
      group.setUrn(actor.toString());
      group.setType(EntityType.CORP_GROUP);
      return group;
    }
    throw new IllegalArgumentException(String.format("Invalid assignee urn: %s", actor));
  }

  private static IncidentSource mapIncidentSource(
      @Nullable QueryContext context, final com.linkedin.incident.IncidentSource incidentSource) {
    final IncidentSource result = new IncidentSource();
    result.setType(IncidentSourceType.valueOf(incidentSource.getType().name()));
    if (incidentSource.hasSourceUrn()) {
      result.setSource(UrnToEntityMapper.map(context, incidentSource.getSourceUrn()));
    }
    return result;
  }

  private IncidentMapper() {}
}
