package com.linkedin.datahub.graphql.types.assertion;

import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionValueChangeType;
import com.linkedin.datahub.graphql.generated.IncrementingSegmentFieldTransformerType;
import com.linkedin.datahub.graphql.generated.VolumeAssertionType;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetFilterMapper;
import javax.annotation.Nullable;

public class VolumeAssertionMapper extends AssertionMapper {

  public static com.linkedin.datahub.graphql.generated.VolumeAssertionInfo mapVolumeAssertionInfo(
      @Nullable final QueryContext context, final VolumeAssertionInfo gmsVolumeAssertionInfo) {
    final com.linkedin.datahub.graphql.generated.VolumeAssertionInfo result =
        new com.linkedin.datahub.graphql.generated.VolumeAssertionInfo();
    result.setEntityUrn(gmsVolumeAssertionInfo.getEntity().toString());
    result.setType(VolumeAssertionType.valueOf(gmsVolumeAssertionInfo.getType().name()));
    if (gmsVolumeAssertionInfo.hasFilter()) {
      result.setFilter(DatasetFilterMapper.map(context, gmsVolumeAssertionInfo.getFilter()));
    }
    if (gmsVolumeAssertionInfo.hasRowCountTotal()) {
      result.setRowCountTotal(mapRowCountTotal(gmsVolumeAssertionInfo.getRowCountTotal()));
    }
    if (gmsVolumeAssertionInfo.hasRowCountChange()) {
      result.setRowCountChange(mapRowCountChange(gmsVolumeAssertionInfo.getRowCountChange()));
    }
    if (gmsVolumeAssertionInfo.hasIncrementingSegmentRowCountTotal()) {
      result.setIncrementingSegmentRowCountTotal(
          mapIncrementingSegmentRowCountTotal(
              gmsVolumeAssertionInfo.getIncrementingSegmentRowCountTotal()));
    }
    if (gmsVolumeAssertionInfo.hasIncrementingSegmentRowCountChange()) {
      result.setIncrementingSegmentRowCountChange(
          mapIncrementingSegmentRowCountChange(
              gmsVolumeAssertionInfo.getIncrementingSegmentRowCountChange()));
    }
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.RowCountTotal mapRowCountTotal(
      final com.linkedin.assertion.RowCountTotal gmsRowCountTotal) {
    final com.linkedin.datahub.graphql.generated.RowCountTotal result =
        new com.linkedin.datahub.graphql.generated.RowCountTotal();
    result.setOperator(AssertionStdOperator.valueOf(gmsRowCountTotal.getOperator().name()));
    result.setParameters(mapParameters(gmsRowCountTotal.getParameters()));
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.RowCountChange mapRowCountChange(
      final com.linkedin.assertion.RowCountChange gmsRowCountChange) {
    final com.linkedin.datahub.graphql.generated.RowCountChange result =
        new com.linkedin.datahub.graphql.generated.RowCountChange();
    result.setOperator(AssertionStdOperator.valueOf(gmsRowCountChange.getOperator().name()));
    result.setParameters(mapParameters(gmsRowCountChange.getParameters()));
    result.setType(AssertionValueChangeType.valueOf(gmsRowCountChange.getType().name()));
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.IncrementingSegmentRowCountTotal
      mapIncrementingSegmentRowCountTotal(
          final com.linkedin.assertion.IncrementingSegmentRowCountTotal
              gmsIncrementingSegmentRowCountTotal) {
    final com.linkedin.datahub.graphql.generated.IncrementingSegmentRowCountTotal result =
        new com.linkedin.datahub.graphql.generated.IncrementingSegmentRowCountTotal();
    result.setOperator(
        AssertionStdOperator.valueOf(gmsIncrementingSegmentRowCountTotal.getOperator().name()));
    result.setParameters(mapParameters(gmsIncrementingSegmentRowCountTotal.getParameters()));
    result.setSegment(mapIncrementingSegmentSpec(gmsIncrementingSegmentRowCountTotal.getSegment()));
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.IncrementingSegmentRowCountChange
      mapIncrementingSegmentRowCountChange(
          final com.linkedin.assertion.IncrementingSegmentRowCountChange
              gmsIncrementingSegmentRowCountChange) {
    final com.linkedin.datahub.graphql.generated.IncrementingSegmentRowCountChange result =
        new com.linkedin.datahub.graphql.generated.IncrementingSegmentRowCountChange();
    result.setOperator(
        AssertionStdOperator.valueOf(gmsIncrementingSegmentRowCountChange.getOperator().name()));
    result.setParameters(mapParameters(gmsIncrementingSegmentRowCountChange.getParameters()));
    result.setSegment(
        mapIncrementingSegmentSpec(gmsIncrementingSegmentRowCountChange.getSegment()));
    result.setType(
        AssertionValueChangeType.valueOf(gmsIncrementingSegmentRowCountChange.getType().name()));
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.IncrementingSegmentSpec
      mapIncrementingSegmentSpec(final com.linkedin.assertion.IncrementingSegmentSpec gmsSegment) {
    final com.linkedin.datahub.graphql.generated.IncrementingSegmentSpec result =
        new com.linkedin.datahub.graphql.generated.IncrementingSegmentSpec();
    result.setField(mapSchemaFieldSpec(gmsSegment.getField()));
    if (gmsSegment.hasTransformer()) {
      result.setTransformer(mapIncrementingSegmentFieldTransformer(gmsSegment.getTransformer()));
    }
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.IncrementingSegmentFieldTransformer
      mapIncrementingSegmentFieldTransformer(
          final com.linkedin.assertion.IncrementingSegmentFieldTransformer gmsTransformer) {
    final com.linkedin.datahub.graphql.generated.IncrementingSegmentFieldTransformer result =
        new com.linkedin.datahub.graphql.generated.IncrementingSegmentFieldTransformer();
    result.setType(
        IncrementingSegmentFieldTransformerType.valueOf(gmsTransformer.getType().name()));
    if (gmsTransformer.hasNativeType()) {
      result.setNativeType(gmsTransformer.getNativeType());
    }
    return result;
  }

  private VolumeAssertionMapper() {}
}
