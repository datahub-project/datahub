package com.linkedin.datahub.graphql.types.anomaly;

import com.linkedin.anomaly.AnomalyInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Anomaly;
import com.linkedin.datahub.graphql.generated.AnomalyReview;
import com.linkedin.datahub.graphql.generated.AnomalyReviewState;
import com.linkedin.datahub.graphql.generated.AnomalySource;
import com.linkedin.datahub.graphql.generated.AnomalySourceType;
import com.linkedin.datahub.graphql.generated.AnomalyState;
import com.linkedin.datahub.graphql.generated.AnomalyStatus;
import com.linkedin.datahub.graphql.generated.AnomalyType;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import javax.annotation.Nullable;

/** Maps a GMS {@link EntityResponse} to a GraphQL Anomaly. */
public class AnomalyMapper {

  public static Anomaly map(
      @Nullable final QueryContext context, final EntityResponse entityResponse) {
    final Anomaly result = new Anomaly();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();
    result.setType(EntityType.ANOMALY);
    result.setUrn(entityUrn.toString());

    final EnvelopedAspect envelopedAnomalyInfo = aspects.get(Constants.ANOMALY_INFO_ASPECT_NAME);
    if (envelopedAnomalyInfo != null) {
      final AnomalyInfo info = new AnomalyInfo(envelopedAnomalyInfo.getValue().data());
      result.setAnomalyType(AnomalyType.valueOf(info.getType().name()));
      if (info.hasDescription()) {
        result.setDescription(info.getDescription(GetMode.NULL));
      }
      result.setEntity(UrnToEntityMapper.map(context, info.getEntity()));
      if (info.hasSeverity()) {
        result.setSeverity(info.getSeverity(GetMode.NULL));
      }
      result.setStatus(mapStatus(context, info.getStatus()));
      result.setReview(mapReview(context, info.getReview()));
      if (info.hasSource()) {
        result.setSource(mapAnomalySource(context, info.getSource()));
      }
      result.setCreated(AuditStampMapper.map(context, info.getCreated()));
    } else {
      throw new RuntimeException(String.format("Anomaly does not exist!. urn: %s", entityUrn));
    }
    return result;
  }

  private static AnomalyStatus mapStatus(
      @Nullable final QueryContext context,
      final com.linkedin.anomaly.AnomalyStatus anomalyStatus) {
    final AnomalyStatus result = new AnomalyStatus();
    result.setState(AnomalyState.valueOf(anomalyStatus.getState().name()));
    result.setLastUpdated(AuditStampMapper.map(context, anomalyStatus.getLastUpdated()));
    return result;
  }

  private static AnomalyReview mapReview(
      @Nullable final QueryContext context,
      final com.linkedin.anomaly.AnomalyReview anomalyReview) {
    final AnomalyReview result = new AnomalyReview();
    result.setState(AnomalyReviewState.valueOf(anomalyReview.getState().name()));
    if (anomalyReview.hasMessage()) {
      result.setMessage(anomalyReview.getMessage(GetMode.NULL));
    }
    result.setLastUpdated(AuditStampMapper.map(context, anomalyReview.getLastUpdated()));
    return result;
  }

  private static AnomalySource mapAnomalySource(
      @Nullable final QueryContext context,
      final com.linkedin.anomaly.AnomalySource anomalySource) {
    final AnomalySource result = new AnomalySource();
    result.setType(AnomalySourceType.valueOf(anomalySource.getType().name()));
    if (anomalySource.hasSourceUrn()) {
      result.setSource(UrnToEntityMapper.map(context, anomalySource.getSourceUrn()));
    }
    return result;
  }

  private AnomalyMapper() {}
}
