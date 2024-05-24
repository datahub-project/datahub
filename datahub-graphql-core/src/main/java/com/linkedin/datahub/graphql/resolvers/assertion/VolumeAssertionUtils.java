package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.assertion.AssertionValueChangeType;
import com.linkedin.assertion.IncrementingSegmentFieldTransformerType;
import com.linkedin.assertion.VolumeAssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CreateVolumeAssertionInput;
import com.linkedin.datahub.graphql.generated.IncrementingSegmentFieldTransformerInput;
import com.linkedin.datahub.graphql.generated.IncrementingSegmentRowCountChangeInput;
import com.linkedin.datahub.graphql.generated.IncrementingSegmentRowCountTotalInput;
import com.linkedin.datahub.graphql.generated.IncrementingSegmentSpecInput;
import com.linkedin.datahub.graphql.generated.RowCountChangeInput;
import com.linkedin.datahub.graphql.generated.RowCountTotalInput;
import javax.annotation.Nonnull;

public class VolumeAssertionUtils {

  @Nonnull
  public static com.linkedin.assertion.IncrementingSegmentSpec createIncrementingSegmentSpec(
      @Nonnull final IncrementingSegmentSpecInput input) {
    final com.linkedin.assertion.IncrementingSegmentSpec result =
        new com.linkedin.assertion.IncrementingSegmentSpec();
    result.setField(AssertionUtils.createSchemaFieldSpec(input.getField()));

    if (input.getTransformer() != null) {
      result.setTransformer(createIncrementingSegmentFieldTransformer(input.getTransformer()));
    }
    return result;
  }

  @Nonnull
  public static com.linkedin.assertion.IncrementingSegmentFieldTransformer
      createIncrementingSegmentFieldTransformer(
          @Nonnull final IncrementingSegmentFieldTransformerInput input) {
    final com.linkedin.assertion.IncrementingSegmentFieldTransformer result =
        new com.linkedin.assertion.IncrementingSegmentFieldTransformer();
    result.setType(IncrementingSegmentFieldTransformerType.valueOf(input.getType().toString()));
    if (input.getNativeType() != null) {
      result.setNativeType(input.getNativeType());
    }
    return result;
  }

  @Nonnull
  public static com.linkedin.assertion.RowCountTotal createRowCountTotal(
      @Nonnull final RowCountTotalInput input) {
    final com.linkedin.assertion.RowCountTotal result = new com.linkedin.assertion.RowCountTotal();
    result.setOperator(AssertionUtils.createAssertionStdOperator(input.getOperator()));
    result.setParameters(AssertionUtils.createDatasetAssertionParameters(input.getParameters()));
    return result;
  }

  @Nonnull
  public static com.linkedin.assertion.RowCountChange createRowCountChange(
      @Nonnull final RowCountChangeInput input) {
    final com.linkedin.assertion.RowCountChange result =
        new com.linkedin.assertion.RowCountChange();
    result.setOperator(AssertionUtils.createAssertionStdOperator(input.getOperator()));
    result.setParameters(AssertionUtils.createDatasetAssertionParameters(input.getParameters()));
    result.setType(AssertionValueChangeType.valueOf(input.getType().toString()));
    return result;
  }

  @Nonnull
  public static com.linkedin.assertion.IncrementingSegmentRowCountTotal
      createIncrementingSegmentRowCountTotal(
          @Nonnull final IncrementingSegmentRowCountTotalInput input) {
    final com.linkedin.assertion.IncrementingSegmentRowCountTotal result =
        new com.linkedin.assertion.IncrementingSegmentRowCountTotal();
    result.setOperator(AssertionUtils.createAssertionStdOperator(input.getOperator()));
    result.setParameters(AssertionUtils.createDatasetAssertionParameters(input.getParameters()));
    result.setSegment(createIncrementingSegmentSpec(input.getSegment()));
    return result;
  }

  @Nonnull
  public static com.linkedin.assertion.IncrementingSegmentRowCountChange
      createIncrementingSegmentRowCountChange(
          @Nonnull final IncrementingSegmentRowCountChangeInput input) {
    final com.linkedin.assertion.IncrementingSegmentRowCountChange result =
        new com.linkedin.assertion.IncrementingSegmentRowCountChange();
    result.setOperator(AssertionUtils.createAssertionStdOperator(input.getOperator()));
    result.setParameters(AssertionUtils.createDatasetAssertionParameters(input.getParameters()));
    result.setSegment(createIncrementingSegmentSpec(input.getSegment()));
    result.setType(AssertionValueChangeType.valueOf(input.getType().toString()));
    return result;
  }

  @Nonnull
  public static com.linkedin.assertion.VolumeAssertionInfo createVolumeAssertionInfo(
      @Nonnull final CreateVolumeAssertionInput input) {
    final com.linkedin.assertion.VolumeAssertionInfo result =
        new com.linkedin.assertion.VolumeAssertionInfo();
    final Urn asserteeUrn = UrnUtils.getUrn(input.getEntityUrn());

    result.setEntity(asserteeUrn);
    result.setType(VolumeAssertionType.valueOf(input.getType().toString()));
    if (input.getFilter() != null) {
      result.setFilter(AssertionUtils.createAssertionFilter(input.getFilter()));
    }

    switch (input.getType()) {
      case ROW_COUNT_TOTAL:
        if (input.getRowCountTotal() != null) {
          result.setRowCountTotal(createRowCountTotal(input.getRowCountTotal()));
        } else {
          throw new DataHubGraphQLException(
              "Invalid input. RowCountTotal is required if VolumeAssertionType is ROW_COUNT_TOTAL.",
              DataHubGraphQLErrorCode.BAD_REQUEST);
        }
        break;
      case ROW_COUNT_CHANGE:
        if (input.getRowCountChange() != null) {
          result.setRowCountChange(createRowCountChange(input.getRowCountChange()));
        } else {
          throw new DataHubGraphQLException(
              "Invalid input. RowCountChange is required if VolumeAssertionType is ROW_COUNT_CHANGE.",
              DataHubGraphQLErrorCode.BAD_REQUEST);
        }
        break;
      case INCREMENTING_SEGMENT_ROW_COUNT_TOTAL:
        if (input.getIncrementingSegmentRowCountTotal() != null) {
          result.setIncrementingSegmentRowCountTotal(
              createIncrementingSegmentRowCountTotal(input.getIncrementingSegmentRowCountTotal()));
        } else {
          throw new DataHubGraphQLException(
              "Invalid input. IncrementingSegmentRowCountTotal is required if VolumeAssertionType is INCREMENTING_SEGMENT_ROW_COUNT_TOTAL.",
              DataHubGraphQLErrorCode.BAD_REQUEST);
        }
        break;
      case INCREMENTING_SEGMENT_ROW_COUNT_CHANGE:
        if (input.getIncrementingSegmentRowCountChange() != null) {
          result.setIncrementingSegmentRowCountChange(
              createIncrementingSegmentRowCountChange(
                  input.getIncrementingSegmentRowCountChange()));
        } else {
          throw new DataHubGraphQLException(
              "Invalid input. IncrementingSegmentRowCountChange is required if VolumeAssertionType is INCREMENTING_SEGMENT_ROW_COUNT_CHANGE.",
              DataHubGraphQLErrorCode.BAD_REQUEST);
        }
        break;
      default:
        throw new RuntimeException(
            String.format("Unsupported Volume Assertion Type %s provided", input.getType()));
    }
    return result;
  }

  private VolumeAssertionUtils() {}
}
