package com.linkedin.datahub.graphql.types.incident;

import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Incident;
import com.linkedin.datahub.graphql.generated.IncidentSource;
import com.linkedin.datahub.graphql.generated.IncidentSourceType;
import com.linkedin.datahub.graphql.generated.IncidentState;
import com.linkedin.datahub.graphql.generated.IncidentStatus;
import com.linkedin.datahub.graphql.generated.IncidentType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.metadata.Constants;
import javax.annotation.Nullable;

/** Maps a GMS {@link EntityResponse} to a GraphQL incident. */
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
      result.setPriority(info.getPriority(GetMode.NULL));
      // TODO: Support multiple entities per incident.
      result.setEntity(UrnToEntityMapper.map(context, info.getEntities().get(0)));
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
    return result;
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
