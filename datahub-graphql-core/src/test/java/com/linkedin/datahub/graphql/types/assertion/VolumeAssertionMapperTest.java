package com.linkedin.datahub.graphql.types.assertion;

import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionValueChangeType;
import com.linkedin.assertion.IncrementingSegmentFieldTransformer;
import com.linkedin.assertion.IncrementingSegmentFieldTransformerType;
import com.linkedin.assertion.IncrementingSegmentRowCountChange;
import com.linkedin.assertion.IncrementingSegmentRowCountTotal;
import com.linkedin.assertion.RowCountChange;
import com.linkedin.assertion.RowCountTotal;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.assertion.VolumeAssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.DatasetFilter;
import com.linkedin.dataset.DatasetFilterType;
import com.linkedin.schema.SchemaFieldSpec;
import org.testng.Assert;
import org.testng.annotations.Test;

public class VolumeAssertionMapperTest {
  @Test
  public void testMapRowCountTotalVolumeAssertionInfo() throws Exception {
    VolumeAssertionInfo volumeAssertionInfo =
        new VolumeAssertionInfo()
            .setEntity(new Urn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)"))
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL)
            .setFilter(
                new DatasetFilter().setType(DatasetFilterType.SQL).setSql("WHERE value > 5;"))
            .setRowCountTotal(
                new RowCountTotal()
                    .setOperator(AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO)
                    .setParameters(
                        new AssertionStdParameters()
                            .setValue(
                                new AssertionStdParameter()
                                    .setType(AssertionStdParameterType.NUMBER)
                                    .setValue("10"))));

    com.linkedin.datahub.graphql.generated.VolumeAssertionInfo result =
        VolumeAssertionMapper.mapVolumeAssertionInfo(null, volumeAssertionInfo);
    Assert.assertEquals(result.getEntityUrn(), "urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)");
    Assert.assertEquals(
        result.getType(),
        com.linkedin.datahub.graphql.generated.VolumeAssertionType.ROW_COUNT_TOTAL);
    Assert.assertEquals(
        result.getFilter().getType(), com.linkedin.datahub.graphql.generated.DatasetFilterType.SQL);
    Assert.assertEquals(result.getFilter().getSql(), "WHERE value > 5;");
    Assert.assertEquals(
        result.getRowCountTotal().getOperator(),
        com.linkedin.datahub.graphql.generated.AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO);
    Assert.assertEquals(
        result.getRowCountTotal().getParameters().getValue().getType(),
        com.linkedin.datahub.graphql.generated.AssertionStdParameterType.NUMBER);
    Assert.assertEquals(result.getRowCountTotal().getParameters().getValue().getValue(), "10");
  }

  @Test
  public void testMapRowCountChangeVolumeAssertionInfo() throws Exception {
    VolumeAssertionInfo volumeAssertionInfo =
        new VolumeAssertionInfo()
            .setEntity(new Urn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)"))
            .setType(VolumeAssertionType.ROW_COUNT_CHANGE)
            .setFilter(
                new DatasetFilter().setType(DatasetFilterType.SQL).setSql("WHERE value > 5;"))
            .setRowCountChange(
                new RowCountChange()
                    .setOperator(AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO)
                    .setParameters(
                        new AssertionStdParameters()
                            .setValue(
                                new AssertionStdParameter()
                                    .setType(AssertionStdParameterType.NUMBER)
                                    .setValue("10")))
                    .setType(AssertionValueChangeType.ABSOLUTE));

    com.linkedin.datahub.graphql.generated.VolumeAssertionInfo result =
        VolumeAssertionMapper.mapVolumeAssertionInfo(null, volumeAssertionInfo);
    Assert.assertEquals(result.getEntityUrn(), "urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)");
    Assert.assertEquals(
        result.getType(),
        com.linkedin.datahub.graphql.generated.VolumeAssertionType.ROW_COUNT_CHANGE);
    Assert.assertEquals(
        result.getFilter().getType(), com.linkedin.datahub.graphql.generated.DatasetFilterType.SQL);
    Assert.assertEquals(result.getFilter().getSql(), "WHERE value > 5;");
    Assert.assertEquals(
        result.getRowCountChange().getOperator(),
        com.linkedin.datahub.graphql.generated.AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO);
    Assert.assertEquals(
        result.getRowCountChange().getParameters().getValue().getType(),
        com.linkedin.datahub.graphql.generated.AssertionStdParameterType.NUMBER);
    Assert.assertEquals(result.getRowCountChange().getParameters().getValue().getValue(), "10");
    Assert.assertEquals(
        result.getRowCountChange().getType(),
        com.linkedin.datahub.graphql.generated.AssertionValueChangeType.ABSOLUTE);
  }

  @Test
  public void testMapIncrementingSegmentRowCountTotalVolumeAssertionInfo() throws Exception {
    VolumeAssertionInfo volumeAssertionInfo =
        new VolumeAssertionInfo()
            .setEntity(new Urn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)"))
            .setType(VolumeAssertionType.INCREMENTING_SEGMENT_ROW_COUNT_TOTAL)
            .setIncrementingSegmentRowCountTotal(
                new IncrementingSegmentRowCountTotal()
                    .setOperator(AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO)
                    .setParameters(
                        new AssertionStdParameters()
                            .setValue(
                                new AssertionStdParameter()
                                    .setType(AssertionStdParameterType.NUMBER)
                                    .setValue("10")))
                    .setSegment(
                        new com.linkedin.assertion.IncrementingSegmentSpec()
                            .setField(
                                new SchemaFieldSpec()
                                    .setPath("path")
                                    .setNativeType("VARCHAR")
                                    .setType("STRING"))
                            .setTransformer(
                                new IncrementingSegmentFieldTransformer()
                                    .setType(IncrementingSegmentFieldTransformerType.CEILING)
                                    .setNativeType("CEILING"))));

    com.linkedin.datahub.graphql.generated.VolumeAssertionInfo result =
        VolumeAssertionMapper.mapVolumeAssertionInfo(null, volumeAssertionInfo);
    Assert.assertEquals(result.getEntityUrn(), "urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)");
    Assert.assertEquals(
        result.getType(),
        com.linkedin.datahub.graphql.generated.VolumeAssertionType
            .INCREMENTING_SEGMENT_ROW_COUNT_TOTAL);
    Assert.assertEquals(
        result.getIncrementingSegmentRowCountTotal().getOperator(),
        com.linkedin.datahub.graphql.generated.AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO);
    Assert.assertEquals(
        result.getIncrementingSegmentRowCountTotal().getParameters().getValue().getType(),
        com.linkedin.datahub.graphql.generated.AssertionStdParameterType.NUMBER);
    Assert.assertEquals(
        result.getIncrementingSegmentRowCountTotal().getParameters().getValue().getValue(), "10");
    Assert.assertEquals(
        result.getIncrementingSegmentRowCountTotal().getSegment().getField().getPath(), "path");
    Assert.assertEquals(
        result.getIncrementingSegmentRowCountTotal().getSegment().getField().getNativeType(),
        "VARCHAR");
    Assert.assertEquals(
        result.getIncrementingSegmentRowCountTotal().getSegment().getField().getType(), "STRING");
    Assert.assertEquals(
        result.getIncrementingSegmentRowCountTotal().getSegment().getTransformer().getType(),
        com.linkedin.datahub.graphql.generated.IncrementingSegmentFieldTransformerType.CEILING);
    Assert.assertEquals(
        result.getIncrementingSegmentRowCountTotal().getSegment().getTransformer().getNativeType(),
        "CEILING");
  }

  @Test
  public void testMapIncrementingSegmentRowCountChangeVolumeAssertionInfo() throws Exception {
    VolumeAssertionInfo volumeAssertionInfo =
        new VolumeAssertionInfo()
            .setEntity(new Urn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)"))
            .setType(VolumeAssertionType.INCREMENTING_SEGMENT_ROW_COUNT_CHANGE)
            .setIncrementingSegmentRowCountChange(
                new IncrementingSegmentRowCountChange()
                    .setType(AssertionValueChangeType.ABSOLUTE)
                    .setOperator(AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO)
                    .setParameters(
                        new AssertionStdParameters()
                            .setValue(
                                new AssertionStdParameter()
                                    .setType(AssertionStdParameterType.NUMBER)
                                    .setValue("10")))
                    .setSegment(
                        new com.linkedin.assertion.IncrementingSegmentSpec()
                            .setField(
                                new SchemaFieldSpec()
                                    .setPath("path")
                                    .setNativeType("VARCHAR")
                                    .setType("STRING"))));

    com.linkedin.datahub.graphql.generated.VolumeAssertionInfo result =
        VolumeAssertionMapper.mapVolumeAssertionInfo(null, volumeAssertionInfo);
    Assert.assertEquals(result.getEntityUrn(), "urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)");
    Assert.assertEquals(
        result.getType(),
        com.linkedin.datahub.graphql.generated.VolumeAssertionType
            .INCREMENTING_SEGMENT_ROW_COUNT_CHANGE);
    Assert.assertEquals(
        result.getIncrementingSegmentRowCountChange().getType(),
        com.linkedin.datahub.graphql.generated.AssertionValueChangeType.ABSOLUTE);
    Assert.assertEquals(
        result.getIncrementingSegmentRowCountChange().getOperator(),
        com.linkedin.datahub.graphql.generated.AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO);
    Assert.assertEquals(
        result.getIncrementingSegmentRowCountChange().getParameters().getValue().getType(),
        com.linkedin.datahub.graphql.generated.AssertionStdParameterType.NUMBER);
    Assert.assertEquals(
        result.getIncrementingSegmentRowCountChange().getParameters().getValue().getValue(), "10");
    Assert.assertEquals(
        result.getIncrementingSegmentRowCountChange().getSegment().getField().getPath(), "path");
    Assert.assertEquals(
        result.getIncrementingSegmentRowCountChange().getSegment().getField().getNativeType(),
        "VARCHAR");
    Assert.assertEquals(
        result.getIncrementingSegmentRowCountChange().getSegment().getField().getType(), "STRING");
  }
}
