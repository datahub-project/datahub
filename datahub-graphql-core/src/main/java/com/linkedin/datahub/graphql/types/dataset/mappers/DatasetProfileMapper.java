package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.PartitionSpec;
import com.linkedin.datahub.graphql.generated.PartitionType;
import com.linkedin.datahub.graphql.generated.TimeWindow;
import com.linkedin.datahub.graphql.types.common.mappers.util.TimeWindowUtils;
import com.linkedin.datahub.graphql.types.mappers.PdlEnumMapper;
import com.linkedin.datahub.graphql.types.mappers.TimeSeriesAspectMapper;
import com.linkedin.dataset.DatasetFieldProfile;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.Quantile;
import com.linkedin.dataset.ValueFrequency;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.utils.GenericRecordUtils;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DatasetProfileMapper
    implements TimeSeriesAspectMapper<com.linkedin.datahub.graphql.generated.DatasetProfile> {

  public static final DatasetProfileMapper INSTANCE = new DatasetProfileMapper();

  public static com.linkedin.datahub.graphql.generated.DatasetProfile map(
      @Nullable QueryContext context, @Nonnull final EnvelopedAspect envelopedAspect) {
    return INSTANCE.apply(context, envelopedAspect);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.DatasetProfile apply(
      @Nullable QueryContext context, @Nonnull final EnvelopedAspect envelopedAspect) {

    DatasetProfile gmsProfile =
        GenericRecordUtils.deserializeAspect(
            envelopedAspect.getAspect().getValue(),
            envelopedAspect.getAspect().getContentType(),
            DatasetProfile.class);

    final com.linkedin.datahub.graphql.generated.DatasetProfile result =
        new com.linkedin.datahub.graphql.generated.DatasetProfile();

    result.setRowCount(gmsProfile.getRowCount());
    result.setColumnCount(gmsProfile.getColumnCount());
    result.setSizeInBytes(gmsProfile.getSizeInBytes());
    result.setTimestampMillis(gmsProfile.getTimestampMillis());
    if (gmsProfile.hasPartitionSpec()) {
      result.setPartitionSpec(mapPartitionSpec(gmsProfile.getPartitionSpec()));
    }
    if (gmsProfile.hasFieldProfiles()) {
      result.setFieldProfiles(
          gmsProfile.getFieldProfiles().stream()
              .map(DatasetProfileMapper::mapFieldProfile)
              .collect(Collectors.toList()));
    }

    return result;
  }

  private PartitionSpec mapPartitionSpec(com.linkedin.timeseries.PartitionSpec partitionSpec) {
    final PartitionSpec result = new PartitionSpec();
    if (partitionSpec.hasPartition()) {
      result.setPartition(partitionSpec.getPartition());
    }
    // GraphQL type is non-null; getType() falls back to the PDL default (PARTITION) when unset,
    // and PdlEnumMapper maps an unrecognized value ($UNKNOWN) to the same default.
    result.setType(
        PdlEnumMapper.map(PartitionType.class, partitionSpec.getType(), PartitionType.PARTITION));
    if (partitionSpec.hasTimePartition()) {
      result.setTimePartition(mapTimePartition(partitionSpec.getTimePartition()));
    }
    return result;
  }

  /**
   * GraphQL TimeWindow requires both startTimeMillis and durationMillis, so the window is omitted
   * when its end cannot be resolved (missing start, length or unit, or an unknown unit).
   */
  @Nullable
  private static TimeWindow mapTimePartition(com.linkedin.timeseries.TimeWindow timePartition) {
    final Long endTimeMillis = TimeWindowUtils.getEndTimeMillis(timePartition);
    if (endTimeMillis == null) {
      return null;
    }
    final TimeWindow result = new TimeWindow();
    result.setStartTimeMillis(timePartition.getStartTimeMillis());
    result.setDurationMillis(endTimeMillis - timePartition.getStartTimeMillis());
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.DatasetFieldProfile mapFieldProfile(
      DatasetFieldProfile gmsProfile) {
    final com.linkedin.datahub.graphql.generated.DatasetFieldProfile result =
        new com.linkedin.datahub.graphql.generated.DatasetFieldProfile();
    result.setFieldPath(gmsProfile.getFieldPath());
    result.setMin(gmsProfile.getMin());
    result.setMax(gmsProfile.getMax());
    result.setStdev(gmsProfile.getStdev());
    result.setMedian(gmsProfile.getMedian());
    result.setMean(gmsProfile.getMean());
    result.setUniqueCount(gmsProfile.getUniqueCount());
    result.setNullCount(gmsProfile.getNullCount());
    if (gmsProfile.hasUniqueProportion()) {
      result.setUniqueProportion(gmsProfile.getUniqueProportion());
    }
    if (gmsProfile.hasNullProportion()) {
      result.setNullProportion(gmsProfile.getNullProportion());
    }
    result.setSampleValues(gmsProfile.getSampleValues());
    if (gmsProfile.hasQuantiles()) {
      result.setQuantiles(
          gmsProfile.getQuantiles().stream()
              .map(DatasetProfileMapper::mapQuantile)
              .collect(Collectors.toList()));
    }
    if (gmsProfile.hasDistinctValueFrequencies()) {
      result.setDistinctValueFrequencies(
          gmsProfile.getDistinctValueFrequencies().stream()
              .map(DatasetProfileMapper::mapValueFrequency)
              .collect(Collectors.toList()));
    }
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.Quantile mapQuantile(Quantile quantile) {
    final com.linkedin.datahub.graphql.generated.Quantile result =
        new com.linkedin.datahub.graphql.generated.Quantile();
    result.setQuantile(quantile.getQuantile());
    result.setValue(quantile.getValue());

    return result;
  }

  private static com.linkedin.datahub.graphql.generated.ValueFrequency mapValueFrequency(
      ValueFrequency frequencies) {
    final com.linkedin.datahub.graphql.generated.ValueFrequency result =
        new com.linkedin.datahub.graphql.generated.ValueFrequency();
    result.setValue(frequencies.getValue());
    result.setFrequency(frequencies.getFrequency());

    return result;
  }
}
