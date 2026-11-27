package com.linkedin.datahub.graphql.types.assertion;

import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionValueChangeType;
import com.linkedin.datahub.graphql.generated.IncrementingSegmentFieldTransformerType;
import com.linkedin.datahub.graphql.generated.VolumeAssertionType;
import com.linkedin.datahub.graphql.types.common.mappers.util.UnknownEnumValues;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetFilterMapper;
import javax.annotation.Nullable;

public class VolumeAssertionMapper extends AssertionMapper {

  public static com.linkedin.datahub.graphql.generated.VolumeAssertionInfo mapVolumeAssertionInfo(
      @Nullable final QueryContext context, final VolumeAssertionInfo gmsVolumeAssertionInfo) {
    final com.linkedin.datahub.graphql.generated.VolumeAssertionInfo result =
        new com.linkedin.datahub.graphql.generated.VolumeAssertionInfo();
    result.setEntityUrn(gmsVolumeAssertionInfo.getEntity().toString());
    result.setType(
        UnknownEnumValues.require(VolumeAssertionType.class, gmsVolumeAssertionInfo.getType()));
    if (gmsVolumeAssertionInfo.hasFilter()) {
      result.setFilter(
          UnknownEnumValues.leaveOutIfUnrepresentable(
              UNREPRESENTABLE,
              "dataset filter",
              () -> DatasetFilterMapper.map(context, gmsVolumeAssertionInfo.getFilter())));
    }
    if (gmsVolumeAssertionInfo.hasRowCountTotal()) {
      result.setRowCountTotal(
          UnknownEnumValues.leaveOutIfUnrepresentable(
              UNREPRESENTABLE,
              "row count total",
              () -> mapRowCountTotal(gmsVolumeAssertionInfo.getRowCountTotal())));
    }
    if (gmsVolumeAssertionInfo.hasRowCountChange()) {
      result.setRowCountChange(
          UnknownEnumValues.leaveOutIfUnrepresentable(
              UNREPRESENTABLE,
              "row count change",
              () -> mapRowCountChange(gmsVolumeAssertionInfo.getRowCountChange())));
    }
    if (gmsVolumeAssertionInfo.hasIncrementingSegmentRowCountTotal()) {
      result.setIncrementingSegmentRowCountTotal(
          UnknownEnumValues.leaveOutIfUnrepresentable(
              UNREPRESENTABLE,
              "incrementing segment row count total",
              () ->
                  mapIncrementingSegmentRowCountTotal(
                      gmsVolumeAssertionInfo.getIncrementingSegmentRowCountTotal())));
    }
    if (gmsVolumeAssertionInfo.hasIncrementingSegmentRowCountChange()) {
      result.setIncrementingSegmentRowCountChange(
          UnknownEnumValues.leaveOutIfUnrepresentable(
              UNREPRESENTABLE,
              "incrementing segment row count change",
              () ->
                  mapIncrementingSegmentRowCountChange(
                      gmsVolumeAssertionInfo.getIncrementingSegmentRowCountChange())));
    }
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.RowCountTotal mapRowCountTotal(
      final com.linkedin.assertion.RowCountTotal gmsRowCountTotal) {
    final com.linkedin.datahub.graphql.generated.RowCountTotal result =
        new com.linkedin.datahub.graphql.generated.RowCountTotal();
    result.setOperator(
        UnknownEnumValues.require(AssertionStdOperator.class, gmsRowCountTotal.getOperator()));
    result.setParameters(mapParameters(gmsRowCountTotal.getParameters()));
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.RowCountChange mapRowCountChange(
      final com.linkedin.assertion.RowCountChange gmsRowCountChange) {
    final com.linkedin.datahub.graphql.generated.RowCountChange result =
        new com.linkedin.datahub.graphql.generated.RowCountChange();
    result.setOperator(
        UnknownEnumValues.require(AssertionStdOperator.class, gmsRowCountChange.getOperator()));
    result.setParameters(mapParameters(gmsRowCountChange.getParameters()));
    result.setType(
        UnknownEnumValues.require(AssertionValueChangeType.class, gmsRowCountChange.getType()));
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.IncrementingSegmentRowCountTotal
      mapIncrementingSegmentRowCountTotal(
          final com.linkedin.assertion.IncrementingSegmentRowCountTotal
              gmsIncrementingSegmentRowCountTotal) {
    final com.linkedin.datahub.graphql.generated.IncrementingSegmentRowCountTotal result =
        new com.linkedin.datahub.graphql.generated.IncrementingSegmentRowCountTotal();
    result.setOperator(
        UnknownEnumValues.require(
            AssertionStdOperator.class, gmsIncrementingSegmentRowCountTotal.getOperator()));
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
        UnknownEnumValues.require(
            AssertionStdOperator.class, gmsIncrementingSegmentRowCountChange.getOperator()));
    result.setParameters(mapParameters(gmsIncrementingSegmentRowCountChange.getParameters()));
    result.setSegment(
        mapIncrementingSegmentSpec(gmsIncrementingSegmentRowCountChange.getSegment()));
    result.setType(
        UnknownEnumValues.require(
            AssertionValueChangeType.class, gmsIncrementingSegmentRowCountChange.getType()));
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.IncrementingSegmentSpec
      mapIncrementingSegmentSpec(final com.linkedin.assertion.IncrementingSegmentSpec gmsSegment) {
    final com.linkedin.datahub.graphql.generated.IncrementingSegmentSpec result =
        new com.linkedin.datahub.graphql.generated.IncrementingSegmentSpec();
    result.setField(mapSchemaFieldSpec(gmsSegment.getField()));
    if (gmsSegment.hasTransformer()) {
      result.setTransformer(
          UnknownEnumValues.leaveOutIfUnrepresentable(
              UNREPRESENTABLE,
              "incrementing segment transformer",
              () -> mapIncrementingSegmentFieldTransformer(gmsSegment.getTransformer())));
    }
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.IncrementingSegmentFieldTransformer
      mapIncrementingSegmentFieldTransformer(
          final com.linkedin.assertion.IncrementingSegmentFieldTransformer gmsTransformer) {
    final com.linkedin.datahub.graphql.generated.IncrementingSegmentFieldTransformer result =
        new com.linkedin.datahub.graphql.generated.IncrementingSegmentFieldTransformer();
    result.setType(
        UnknownEnumValues.require(
            IncrementingSegmentFieldTransformerType.class, gmsTransformer.getType()));
    if (gmsTransformer.hasNativeType()) {
      result.setNativeType(gmsTransformer.getNativeType());
    }
    return result;
  }

  private VolumeAssertionMapper() {}
}
