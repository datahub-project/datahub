package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.datahub.graphql.types.mappers.TimeSeriesAspectMapper;
import com.linkedin.dataset.DatasetFieldProfile;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.utils.GenericRecordUtils;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class DatasetProfileMapper implements TimeSeriesAspectMapper<com.linkedin.datahub.graphql.generated.DatasetProfile> {

  public static final DatasetProfileMapper INSTANCE = new DatasetProfileMapper();

  public static com.linkedin.datahub.graphql.generated.DatasetProfile map(@Nonnull final EnvelopedAspect envelopedAspect) {
    return INSTANCE.apply(envelopedAspect);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.DatasetProfile apply(@Nonnull final EnvelopedAspect envelopedAspect) {

    DatasetProfile gmsProfile = GenericRecordUtils
        .deserializeAspect(
            envelopedAspect.getAspect().getValue(),
            envelopedAspect.getAspect().getContentType(),
            DatasetProfile.class);

    final com.linkedin.datahub.graphql.generated.DatasetProfile result =
        new com.linkedin.datahub.graphql.generated.DatasetProfile();

    result.setRowCount(gmsProfile.getRowCount());
    result.setColumnCount(gmsProfile.getColumnCount());
    result.setTimestampMillis(gmsProfile.getTimestampMillis());
    if (gmsProfile.hasFieldProfiles()) {
      result.setFieldProfiles(
          gmsProfile.getFieldProfiles().stream().map(DatasetProfileMapper::mapFieldProfile).collect(Collectors.toList()));
    }

    return result;
  }

  private static com.linkedin.datahub.graphql.generated.DatasetFieldProfile mapFieldProfile(DatasetFieldProfile gmsProfile) {
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
    return result;
  }
}
