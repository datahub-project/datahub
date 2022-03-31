package com.linkedin.datahub.graphql.types.incident;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.generated.Incident;
import com.linkedin.datahub.graphql.generated.IncidentSource;
import com.linkedin.datahub.graphql.generated.IncidentSourceType;
import com.linkedin.datahub.graphql.generated.IncidentState;
import com.linkedin.datahub.graphql.generated.IncidentStatus;
import com.linkedin.datahub.graphql.generated.IncidentType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.metadata.Constants;


/**
 * Maps a GMS {@link EntityResponse} to a GraphQL incident.
 */
public class IncidentMapper {

  public static Incident map(final EntityResponse entityResponse) {
    final Incident result = new Incident();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());

    final EnvelopedAspect envelopedIncidentInfo = aspects.get(Constants.INCIDENT_INFO_ASPECT_NAME);
    if (envelopedIncidentInfo != null) {
      final IncidentInfo info = new IncidentInfo(envelopedIncidentInfo.getValue().data());
      result.setType(IncidentType.valueOf(info.getType().name())); // Assumption alert! This assumes the incident type in GMS exactly equals that in GraphQL.
      result.setCustomType(info.getCustomType(GetMode.NULL));
      result.setTitle(info.getTitle(GetMode.NULL));
      result.setDescription(info.getDescription(GetMode.NULL));
      // TODO: Support multiple entities per incident.
      result.setEntity(UrnToEntityMapper.map(info.getEntities().get(0)));
      if (info.hasSource()) {
        result.setSource(mapIncidentSource(info.getSource()));
      }
      if (info.hasStatus()) {
        result.setStatus(mapStatus(info.getStatus()));
      }
      result.setCreated(AuditStampMapper.map(info.getCreated()));
    } else {
      throw new RuntimeException(String.format("Incident does not exist!. urn: %s", entityUrn));
    }
    return result;
  }

  private static IncidentStatus mapStatus(final com.linkedin.incident.IncidentStatus incidentStatus) {
    final IncidentStatus result = new IncidentStatus();
    result.setState(IncidentState.valueOf(incidentStatus.getState().name()));
    result.setMessage(incidentStatus.getMessage(GetMode.NULL));
    result.setLastUpdated(AuditStampMapper.map(incidentStatus.getLastUpdated()));
    return result;
  }

  private static IncidentSource mapIncidentSource(final com.linkedin.incident.IncidentSource incidentSource) {
    final IncidentSource result = new IncidentSource();
    result.setType(IncidentSourceType.valueOf(incidentSource.getType().name()));
    return result;
  }

  private IncidentMapper() {
  }
}