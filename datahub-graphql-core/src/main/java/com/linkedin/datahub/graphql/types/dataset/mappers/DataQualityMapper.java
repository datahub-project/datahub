package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ChangeAuditStamps;
import com.linkedin.datahub.graphql.generated.DimensionScore;
import com.linkedin.datahub.graphql.generated.SchemaFieldQualityDimensionInfo;
import com.linkedin.datahub.graphql.generated.ScoreType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.dataquality.DataQuality;
import com.linkedin.dataquality.DataQualityDimensionInfo;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataQualityMapper {
  public static final DataQualityMapper INSTANCE = new DataQualityMapper();

  public static com.linkedin.datahub.graphql.generated.DataQuality map(
      @Nullable QueryContext context, @Nonnull final DataQuality dataQuality) {

    return INSTANCE.apply(context, dataQuality);
  }

  public com.linkedin.datahub.graphql.generated.DataQuality apply(
      @Nullable QueryContext context, @Nonnull final DataQuality input) {
    final com.linkedin.datahub.graphql.generated.DataQuality dataQuality =
        new com.linkedin.datahub.graphql.generated.DataQuality();
    if (input.hasDatasetDimensionInfo()) {
      dataQuality.setDatasetDimensionInfo(
          mapDataQualityDimensionInfo(input.getDatasetDimensionInfo()));
    }
    if (input.hasSchemaFieldDimensionInfos()) {
      dataQuality.setSchemaFieldDimensionInfos(mapSchemaFieldQualityDimensionInfo(context, input));
    }
    if (input.hasCreated() || input.hasDeleted() || input.hasLastModified()) {
      dataQuality.setChangeAuditStamps(
          mapChangeAuditStamps(
              context, input.getCreated(), input.getDeleted(), input.getLastModified()));
    }
    return dataQuality;
  }

  private com.linkedin.datahub.graphql.generated.DataQualityDimensionInfo
      mapDataQualityDimensionInfo(@Nonnull final DataQualityDimensionInfo input) {
    com.linkedin.datahub.graphql.generated.DataQualityDimensionInfo dataQualityDimensionInfo =
        new com.linkedin.datahub.graphql.generated.DataQualityDimensionInfo();

    if (input.hasNote()) {
      dataQualityDimensionInfo.setNote(input.getNote());
    }
    if (input.hasToolName()) {
      dataQualityDimensionInfo.setToolName(input.getToolName());
    }
    if (input.hasRecordCount()) {
      dataQualityDimensionInfo.setRecordCount(input.getRecordCount());
    }

    List<DimensionScore> dimensionsList = new ArrayList<>();
    if (input.hasDimensions()) {
      input.getDimensions().stream()
          .forEach(
              item -> {
                DimensionScore dimensionScore = new DimensionScore();
                if (item.hasNote()) {
                  dimensionScore.setNote(item.getNote());
                }
                dimensionScore.setCurrentScore(item.getCurrentScore());
                dimensionScore.setScoreType(ScoreType.valueOf(item.getScoreType().toString()));
                if (item.hasHistoricalWeightedScore()) {
                  dimensionScore.setHistoricalWeightedScore(item.getHistoricalWeightedScore());
                }
                dimensionScore.setDimensionUrn(item.getDimensionUrn().toString());
                dimensionsList.add(dimensionScore);
              });
    }
    dataQualityDimensionInfo.setDimensions(dimensionsList);
    return dataQualityDimensionInfo;
  }

  private List<SchemaFieldQualityDimensionInfo> mapSchemaFieldQualityDimensionInfo(
      QueryContext context, @Nonnull final DataQuality input) {
    List<SchemaFieldQualityDimensionInfo> schemaFieldQualityDimensionInfoList = new ArrayList<>();
    if (input.hasSchemaFieldDimensionInfos()) {
      input.getSchemaFieldDimensionInfos().stream()
          .forEach(
              item -> {
                SchemaFieldQualityDimensionInfo schemaInfo = new SchemaFieldQualityDimensionInfo();
                schemaInfo.setSchemaFieldUrn(item.getSchemaFieldUrn().toString());
                if (item.hasSchemaFieldDimensionInfo()) {
                  schemaInfo.setSchemaFieldDimensionInfo(
                      mapDataQualityDimensionInfo(item.getSchemaFieldDimensionInfo()));
                }
                if (item.hasCreated() || item.hasDeleted() || item.hasLastModified()) {
                  schemaInfo.setChangeAuditStamps(
                      mapChangeAuditStamps(
                          context, item.getCreated(), item.getDeleted(), item.getLastModified()));
                }
                schemaFieldQualityDimensionInfoList.add(schemaInfo);
              });
    }
    return schemaFieldQualityDimensionInfoList;
  }

  private ChangeAuditStamps mapChangeAuditStamps(
      QueryContext context,
      @Nonnull AuditStamp created,
      @Nonnull AuditStamp deleted,
      @Nonnull AuditStamp lastModified) {
    ChangeAuditStamps changeAuditStamps = new ChangeAuditStamps();
    if (created != null) {
      changeAuditStamps.setCreated(AuditStampMapper.map(context, created));
    }
    if (deleted != null) {
      changeAuditStamps.setDeleted(AuditStampMapper.map(context, deleted));
    }
    if (lastModified != null) {
      changeAuditStamps.setLastModified(AuditStampMapper.map(context, lastModified));
    }
    return changeAuditStamps;
  }
}
