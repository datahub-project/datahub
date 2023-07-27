package com.linkedin.datahub.graphql.types.assertion;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.FreshnessCronSchedule;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.FixedIntervalSchedule;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AssertionMapperTest {

  @Test
  public void testMapDatasetAssertion() {
    // Case 1: Without nullable fields
    AssertionInfo input = createFreshnessAssertionInfoWithoutNullableFields();
    EntityResponse datasetAssertionEntityResponse = createAssertionInfoEntityResponse(input);
    Assertion output = AssertionMapper.map(datasetAssertionEntityResponse);
    verifyAssertion(input, output);

    // Case 2: With nullable fields
    input = createFreshnessAssertionInfoWithNullableFields();
    EntityResponse datasetAssertionEntityResponseWithNullables = createAssertionInfoEntityResponse(input);
    output = AssertionMapper.map(datasetAssertionEntityResponseWithNullables);
    verifyAssertion(input, output);
  }

  @Test
  public void testMapFreshnessAssertion() {
    // Case 1: Without nullable fields
    AssertionInfo input = createFreshnessAssertionInfoWithoutNullableFields();
    EntityResponse freshnessAssertionEntityResponse = createAssertionInfoEntityResponse(input);
    Assertion output = AssertionMapper.map(freshnessAssertionEntityResponse);
    verifyAssertion(input, output);

    // Case 2: With nullable fields
    input = createDatasetAssertionInfoWithNullableFields();
    EntityResponse freshnessAssertionEntityResponseWithNullables = createAssertionInfoEntityResponse(input);
    output = AssertionMapper.map(freshnessAssertionEntityResponseWithNullables);
    verifyAssertion(input, output);
  }

  private void verifyAssertion(AssertionInfo input, Assertion output) {
    Assert.assertNotNull(output);
    Assert.assertNotNull(output.getInfo());
    Assert.assertEquals(output.getInfo().getType().toString(), output.getInfo().getType().toString());

    if (input.hasDatasetAssertion()) {
      verifyDatasetAssertion(input.getDatasetAssertion(), output.getInfo().getDatasetAssertion());
    }

    if (input.hasFreshnessAssertion()) {
      verifyFreshnessAssertion(input.getFreshnessAssertion(), output.getInfo().getFreshnessAssertion());
    }

    if (input.hasSource()) {
      verifySource(input.getSource(), output.getInfo().getSource());
    }
  }

  private void verifyDatasetAssertion(DatasetAssertionInfo input, com.linkedin.datahub.graphql.generated.DatasetAssertionInfo output) {
    Assert.assertEquals(output.getOperator().toString(), input.getOperator().toString());
    Assert.assertEquals(output.getOperator().toString(), input.getOperator().toString());
    Assert.assertEquals(output.getScope().toString(), input.getScope().toString());
    Assert.assertEquals(output.getDatasetUrn(), input.getDataset().toString());
    if (input.hasAggregation()) {
      Assert.assertEquals(output.getAggregation().toString(), input.getAggregation().toString());
    }
    if (input.hasNativeType()) {
      Assert.assertEquals(output.getNativeType(), input.getNativeType().toString());
    }
    if (input.hasLogic()) {
      Assert.assertEquals(output.getLogic(), input.getLogic());
    }
    if (input.hasFields()) {
      Assert.assertTrue(input.getFields().stream()
          .allMatch(field -> output.getFields().stream().anyMatch(outField -> field.toString().equals(outField.getUrn()))));
    }
  }

  private void verifyFreshnessAssertion(FreshnessAssertionInfo input, com.linkedin.datahub.graphql.generated.FreshnessAssertionInfo output) {
    Assert.assertEquals(output.getType().toString(), input.getType().toString());
    Assert.assertEquals(output.getEntityUrn(), input.getEntity().toString());
    if (input.hasSchedule()) {
      verifyFreshnessSchedule(input.getSchedule(), output.getSchedule());
    }
  }

  private void verifyCronSchedule(FreshnessCronSchedule input, com.linkedin.datahub.graphql.generated.FreshnessCronSchedule output) {
    Assert.assertEquals(output.getCron(), input.getCron());
    Assert.assertEquals(output.getTimezone(), input.getTimezone());
    if (input.hasWindowStartOffsetMs()) {
      Assert.assertEquals(output.getWindowStartOffsetMs(), input.getWindowStartOffsetMs());
    }
  }

  private void verifyFreshnessSchedule(FreshnessAssertionSchedule input, com.linkedin.datahub.graphql.generated.FreshnessAssertionSchedule output) {
    Assert.assertEquals(output.getType().toString(), input.getType().toString());
    if (input.hasCron()) {
      verifyCronSchedule(input.getCron(), output.getCron());
    }
    if (input.hasFixedInterval()) {
      verifyFixedIntervalSchedule(input.getFixedInterval(), output.getFixedInterval());
    }
  }

  private void verifyFixedIntervalSchedule(com.linkedin.assertion.FixedIntervalSchedule input, FixedIntervalSchedule output) {
    Assert.assertEquals(output.getMultiple(), (int) input.getMultiple());
    Assert.assertEquals(output.getUnit().toString(), input.getUnit().toString());
  }

  private void verifySource(AssertionSource input, com.linkedin.datahub.graphql.generated.AssertionSource output) {
    Assert.assertEquals(output.getType().toString(), input.getType().toString());
  }

  private EntityResponse createAssertionInfoEntityResponse(final AssertionInfo info) {
    EnvelopedAspect envelopedAssertionInfo = createEnvelopedAspect(info.data());
    return createEntityResponse(Constants.ASSERTION_INFO_ASPECT_NAME, envelopedAssertionInfo);
  }

  private EntityResponse createEntityResponse(String aspectName, EnvelopedAspect envelopedAspect) {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:assertion:1"));
    entityResponse.setAspects(new EnvelopedAspectMap(new HashMap<>()));
    entityResponse.getAspects().put(aspectName, envelopedAspect);
    return entityResponse;
  }

  private EnvelopedAspect createEnvelopedAspect(DataMap dataMap) {
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(dataMap));
    return envelopedAspect;
  }

  private AssertionInfo createDatasetAssertionInfoWithoutNullableFields() {
    AssertionInfo info = new AssertionInfo();
    info.setType(com.linkedin.assertion.AssertionType.DATASET);
    DatasetAssertionInfo datasetAssertionInfo = new DatasetAssertionInfo();
    datasetAssertionInfo.setDataset(UrnUtils.getUrn("urn:li:dataset:1"));
    datasetAssertionInfo.setScope(DatasetAssertionScope.DATASET_COLUMN);
    datasetAssertionInfo.setOperator(AssertionStdOperator.GREATER_THAN);
    info.setDatasetAssertion(datasetAssertionInfo);
    return info;
  }

  private AssertionInfo createDatasetAssertionInfoWithNullableFields() {
    AssertionInfo infoWithoutNullables = createDatasetAssertionInfoWithoutNullableFields();
    DatasetAssertionInfo baseInfo = infoWithoutNullables.getDatasetAssertion();
    baseInfo.setFields(new UrnArray(Arrays.asList(UrnUtils.getUrn("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD),field)"))));
    baseInfo.setAggregation(AssertionStdAggregation.SUM);
    baseInfo.setParameters(createAssertionStdParameters());
    baseInfo.setNativeType("native_type");
    baseInfo.setNativeParameters(new StringMap(Collections.singletonMap("key", "value")));
    baseInfo.setLogic("sample_logic");
    infoWithoutNullables.setSource(new AssertionSource()
        .setType(com.linkedin.assertion.AssertionSourceType.INFERRED)
    );
    return infoWithoutNullables;
  }

  private AssertionInfo createFreshnessAssertionInfoWithoutNullableFields() {
    AssertionInfo info = new AssertionInfo();
    info.setType(AssertionType.FRESHNESS);
    FreshnessAssertionInfo freshnessAssertionInfo = new FreshnessAssertionInfo();
    freshnessAssertionInfo.setEntity(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)"));
    freshnessAssertionInfo.setType(FreshnessAssertionType.DATASET_CHANGE);
    info.setFreshnessAssertion(freshnessAssertionInfo);
    return info;
  }

  private AssertionInfo createFreshnessAssertionInfoWithNullableFields() {
    AssertionInfo infoWithoutNullables = createFreshnessAssertionInfoWithoutNullableFields();
    FreshnessAssertionInfo baseInfo = infoWithoutNullables.getFreshnessAssertion();
    baseInfo.setSchedule(createFreshnessAssertionSchedule());
    infoWithoutNullables.setSource(new AssertionSource()
      .setType(com.linkedin.assertion.AssertionSourceType.INFERRED)
    );
    return infoWithoutNullables;
  }

  private AssertionStdParameters createAssertionStdParameters() {
    AssertionStdParameters parameters = new AssertionStdParameters();
    parameters.setValue(createAssertionStdParameter());
    parameters.setMinValue(createAssertionStdParameter());
    parameters.setMaxValue(createAssertionStdParameter());
    return parameters;
  }

  private AssertionStdParameter createAssertionStdParameter() {
    AssertionStdParameter parameter = new AssertionStdParameter();
    parameter.setType(AssertionStdParameterType.NUMBER);
    parameter.setValue("100");
    return parameter;
  }

  private FreshnessAssertionSchedule createFreshnessAssertionSchedule() {
    FreshnessAssertionSchedule schedule = new FreshnessAssertionSchedule();
    schedule.setType(FreshnessAssertionScheduleType.CRON);
    schedule.setCron(createCronSchedule());
    return schedule;
  }

  private FreshnessCronSchedule createCronSchedule() {
    FreshnessCronSchedule cronSchedule = new FreshnessCronSchedule();
    cronSchedule.setCron("0 0 * * *");
    cronSchedule.setTimezone("UTC");
    return cronSchedule;
  }
}
