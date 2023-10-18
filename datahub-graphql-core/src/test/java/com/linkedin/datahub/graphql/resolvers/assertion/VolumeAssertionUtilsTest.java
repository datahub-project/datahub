package com.linkedin.datahub.graphql.resolvers.assertion;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterInput;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterType;
import com.linkedin.datahub.graphql.generated.AssertionStdParametersInput;
import com.linkedin.datahub.graphql.generated.AssertionValueChangeType;
import com.linkedin.datahub.graphql.generated.CreateVolumeAssertionInput;
import com.linkedin.datahub.graphql.generated.DatasetFilterInput;
import com.linkedin.datahub.graphql.generated.DatasetFilterType;
import com.linkedin.datahub.graphql.generated.IncrementingSegmentFieldTransformerInput;
import com.linkedin.datahub.graphql.generated.IncrementingSegmentFieldTransformerType;
import com.linkedin.datahub.graphql.generated.IncrementingSegmentRowCountChangeInput;
import com.linkedin.datahub.graphql.generated.IncrementingSegmentRowCountTotalInput;
import com.linkedin.datahub.graphql.generated.IncrementingSegmentSpecInput;
import com.linkedin.datahub.graphql.generated.RowCountChangeInput;
import com.linkedin.datahub.graphql.generated.RowCountTotalInput;
import com.linkedin.datahub.graphql.generated.SchemaFieldSpecInput;
import com.linkedin.datahub.graphql.generated.VolumeAssertionType;

public class VolumeAssertionUtilsTest {
    @Test
    public void testCreateIncrementingSegmentSpec() {
        IncrementingSegmentSpecInput input = new IncrementingSegmentSpecInput();
        SchemaFieldSpecInput field = new SchemaFieldSpecInput();
        field.setPath("path");
        field.setType("STRING");
        field.setNativeType("VARCHAR");
        input.setField(field);

        com.linkedin.assertion.IncrementingSegmentSpec result = VolumeAssertionUtils
                .createIncrementingSegmentSpec(input);
        Assert.assertEquals(result.getField().getPath(), "path");
        Assert.assertEquals(result.getField().getType(), "STRING");
        Assert.assertEquals(result.getField().getNativeType(), "VARCHAR");
    }

    @Test
    public void testCreateIncrementingSegmentFieldTransformer() {
        IncrementingSegmentFieldTransformerInput input = new IncrementingSegmentFieldTransformerInput();
        input.setType(IncrementingSegmentFieldTransformerType.FLOOR);
        input.setNativeType("FLOOR");

        com.linkedin.assertion.IncrementingSegmentFieldTransformer result = VolumeAssertionUtils
                .createIncrementingSegmentFieldTransformer(input);
        Assert.assertEquals(result.getType(), com.linkedin.assertion.IncrementingSegmentFieldTransformerType.FLOOR);
        Assert.assertEquals(result.getNativeType(), "FLOOR");
    }

    @Test
    public void testCreateRowCountTotal() {
        RowCountTotalInput input = new RowCountTotalInput();
        AssertionStdParametersInput parameters = new AssertionStdParametersInput();
        AssertionStdParameterInput parameter = new AssertionStdParameterInput();
        parameter.setType(AssertionStdParameterType.NUMBER);
        parameter.setValue("1");
        parameters.setValue(parameter);
        input.setOperator(AssertionStdOperator.GREATER_THAN);
        input.setParameters(parameters);

        com.linkedin.assertion.RowCountTotal result = VolumeAssertionUtils.createRowCountTotal(input);
        Assert.assertEquals(result.getOperator(), com.linkedin.assertion.AssertionStdOperator.GREATER_THAN);
        Assert.assertEquals(result.getParameters().getValue().getType(),
                com.linkedin.assertion.AssertionStdParameterType.NUMBER);
        Assert.assertEquals(result.getParameters().getValue().getValue(), "1");
    }

    @Test
    public void testCreateRowCountChange() {
        RowCountChangeInput input = new RowCountChangeInput();
        AssertionStdParametersInput parameters = new AssertionStdParametersInput();
        AssertionStdParameterInput parameter = new AssertionStdParameterInput();
        parameter.setType(AssertionStdParameterType.NUMBER);
        parameter.setValue("1");
        parameters.setValue(parameter);
        input.setOperator(AssertionStdOperator.GREATER_THAN);
        input.setParameters(parameters);
        input.setType(AssertionValueChangeType.ABSOLUTE);

        com.linkedin.assertion.RowCountChange result = VolumeAssertionUtils.createRowCountChange(input);
        Assert.assertEquals(result.getOperator(), com.linkedin.assertion.AssertionStdOperator.GREATER_THAN);
        Assert.assertEquals(result.getParameters().getValue().getType(),
                com.linkedin.assertion.AssertionStdParameterType.NUMBER);
        Assert.assertEquals(result.getParameters().getValue().getValue(), "1");
        Assert.assertEquals(result.getType(), com.linkedin.assertion.AssertionValueChangeType.ABSOLUTE);
    }

    @Test
    public void testCreateIncrementingSegmentRowCountTotal() {
        IncrementingSegmentRowCountTotalInput input = new IncrementingSegmentRowCountTotalInput();
        AssertionStdParametersInput parameters = new AssertionStdParametersInput();
        AssertionStdParameterInput parameter = new AssertionStdParameterInput();
        IncrementingSegmentSpecInput segment = new IncrementingSegmentSpecInput();
        IncrementingSegmentFieldTransformerInput transformer = new IncrementingSegmentFieldTransformerInput();
        SchemaFieldSpecInput field = new SchemaFieldSpecInput();
        parameter.setType(AssertionStdParameterType.NUMBER);
        parameter.setValue("1");
        parameters.setValue(parameter);
        transformer.setType(IncrementingSegmentFieldTransformerType.FLOOR);
        transformer.setNativeType("FLOOR");
        field.setPath("path");
        field.setType("STRING");
        field.setNativeType("VARCHAR");
        segment.setTransformer(transformer);
        segment.setField(field);
        input.setOperator(AssertionStdOperator.GREATER_THAN);
        input.setParameters(parameters);
        input.setSegment(segment);

        com.linkedin.assertion.IncrementingSegmentRowCountTotal result = VolumeAssertionUtils
                .createIncrementingSegmentRowCountTotal(input);
        Assert.assertEquals(result.getOperator(), com.linkedin.assertion.AssertionStdOperator.GREATER_THAN);
        Assert.assertEquals(result.getParameters().getValue().getType(),
                com.linkedin.assertion.AssertionStdParameterType.NUMBER);
        Assert.assertEquals(result.getParameters().getValue().getValue(), "1");
        Assert.assertEquals(result.getSegment().getField().getPath(), "path");
        Assert.assertEquals(result.getSegment().getField().getType(), "STRING");
        Assert.assertEquals(result.getSegment().getField().getNativeType(), "VARCHAR");
        Assert.assertEquals(result.getSegment().getTransformer().getType(),
                com.linkedin.assertion.IncrementingSegmentFieldTransformerType.FLOOR);
        Assert.assertEquals(result.getSegment().getTransformer().getNativeType(), "FLOOR");
    }

    @Test
    public void testCreateIncrementingSegmentRowCountChange() {
        IncrementingSegmentRowCountChangeInput input = new IncrementingSegmentRowCountChangeInput();
        AssertionStdParametersInput parameters = new AssertionStdParametersInput();
        AssertionStdParameterInput parameter = new AssertionStdParameterInput();
        IncrementingSegmentSpecInput segment = new IncrementingSegmentSpecInput();
        IncrementingSegmentFieldTransformerInput transformer = new IncrementingSegmentFieldTransformerInput();
        SchemaFieldSpecInput field = new SchemaFieldSpecInput();
        parameter.setType(AssertionStdParameterType.NUMBER);
        parameter.setValue("1");
        parameters.setValue(parameter);
        transformer.setType(IncrementingSegmentFieldTransformerType.FLOOR);
        transformer.setNativeType("FLOOR");
        field.setPath("path");
        field.setType("STRING");
        field.setNativeType("VARCHAR");
        segment.setTransformer(transformer);
        segment.setField(field);
        input.setOperator(AssertionStdOperator.GREATER_THAN);
        input.setParameters(parameters);
        input.setSegment(segment);
        input.setType(AssertionValueChangeType.ABSOLUTE);

        com.linkedin.assertion.IncrementingSegmentRowCountChange result = VolumeAssertionUtils
                .createIncrementingSegmentRowCountChange(input);
        Assert.assertEquals(result.getOperator(), com.linkedin.assertion.AssertionStdOperator.GREATER_THAN);
        Assert.assertEquals(result.getParameters().getValue().getType(),
                com.linkedin.assertion.AssertionStdParameterType.NUMBER);
        Assert.assertEquals(result.getParameters().getValue().getValue(), "1");
        Assert.assertEquals(result.getSegment().getField().getPath(), "path");
        Assert.assertEquals(result.getSegment().getField().getType(), "STRING");
        Assert.assertEquals(result.getSegment().getField().getNativeType(), "VARCHAR");
        Assert.assertEquals(result.getSegment().getTransformer().getType(),
                com.linkedin.assertion.IncrementingSegmentFieldTransformerType.FLOOR);
        Assert.assertEquals(result.getSegment().getTransformer().getNativeType(), "FLOOR");
        Assert.assertEquals(result.getType(), com.linkedin.assertion.AssertionValueChangeType.ABSOLUTE);
    }

    @Test
    public void testCreateVolumeAssertionInfo() {
        CreateVolumeAssertionInput input = new CreateVolumeAssertionInput();
        RowCountTotalInput rowCountTotal = new RowCountTotalInput();
        AssertionStdParametersInput parameters = new AssertionStdParametersInput();
        AssertionStdParameterInput parameter = new AssertionStdParameterInput();
        DatasetFilterInput filter = new DatasetFilterInput();
        filter.setSql("WHERE value = 1;");
        filter.setType(DatasetFilterType.SQL);
        parameter.setType(AssertionStdParameterType.NUMBER);
        parameter.setValue("1");
        parameters.setValue(parameter);
        rowCountTotal.setOperator(AssertionStdOperator.GREATER_THAN);
        rowCountTotal.setParameters(parameters);
        input.setType(VolumeAssertionType.ROW_COUNT_TOTAL);
        input.setRowCountTotal(rowCountTotal);
        input.setFilter(filter);
        input.setEntityUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,dataset1,PROD)");

        com.linkedin.assertion.VolumeAssertionInfo result = VolumeAssertionUtils.createVolumeAssertionInfo(input);
        Assert.assertEquals(result.getType(), com.linkedin.assertion.VolumeAssertionType.ROW_COUNT_TOTAL);
        Assert.assertEquals(result.getRowCountTotal().getOperator(),
                com.linkedin.assertion.AssertionStdOperator.GREATER_THAN);
        Assert.assertEquals(result.getRowCountTotal().getParameters().getValue().getType(),
                com.linkedin.assertion.AssertionStdParameterType.NUMBER);
        Assert.assertEquals(result.getRowCountTotal().getParameters().getValue().getValue(), "1");
        Assert.assertEquals(result.getFilter().getSql(), "WHERE value = 1;");
        Assert.assertEquals(result.getFilter().getType(), com.linkedin.dataset.DatasetFilterType.SQL);
    }
}
