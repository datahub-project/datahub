package com.linkedin.datahub.graphql.types.assertion;

import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionNote;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.FreshnessCronSchedule;
import com.linkedin.assertion.SchemaAssertionCompatibility;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.TagUrn;
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
import com.linkedin.schema.MySqlDDL;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AssertionMapperTest {

  @Test
  public void testMapDatasetAssertion() {
    // Case 1: Without nullable fields
    AssertionInfo input = createFreshnessAssertionInfoWithoutNullableFields();
    EntityResponse datasetAssertionEntityResponse = createAssertionInfoEntityResponse(input);
    Assertion output = AssertionMapper.map(null, datasetAssertionEntityResponse);
    verifyAssertionInfo(input, output);

    // Case 2: With nullable fields
    input = createFreshnessAssertionInfoWithNullableFields();
    EntityResponse datasetAssertionEntityResponseWithNullables =
        createAssertionInfoEntityResponse(input);
    output = AssertionMapper.map(null, datasetAssertionEntityResponseWithNullables);
    verifyAssertionInfo(input, output);
  }

  @Test
  public void testMapTags() throws Exception {
    HashMap<String, EnvelopedAspect> aspects = new HashMap<>();
    AssertionInfo info = createFreshnessAssertionInfoWithoutNullableFields();

    EnvelopedAspect envelopedTagsAspect = new EnvelopedAspect();
    GlobalTags tags = new GlobalTags();
    tags.setTags(
        new TagAssociationArray(
            new TagAssociationArray(
                Collections.singletonList(
                    new com.linkedin.common.TagAssociation()
                        .setTag(TagUrn.createFromString("urn:li:tag:test"))))));
    envelopedTagsAspect.setValue(new Aspect(tags.data()));

    aspects.put(Constants.ASSERTION_INFO_ASPECT_NAME, createEnvelopedAspect(info.data()));
    aspects.put(Constants.GLOBAL_TAGS_ASPECT_NAME, createEnvelopedAspect(tags.data()));
    EntityResponse response = createEntityResponse(aspects);

    Assertion assertion = AssertionMapper.map(null, response);
    assertEquals(assertion.getTags().getTags().size(), 1);
    assertEquals(
        assertion.getTags().getTags().get(0).getTag().getUrn().toString(), "urn:li:tag:test");
  }

  @Test
  public void testMapAssertionNoEntity() {
    AssertionInfo inputInfoWithNullEntity = new AssertionInfo().setType(AssertionType.FRESHNESS);
    EntityResponse response = createAssertionInfoEntityResponse(inputInfoWithNullEntity);
    try {
      AssertionMapper.map(null, response);
    } catch (Exception e) {
      Assert.fail("Mapping failed with null entity in FreshnessAssertionInfo");
    }
  }

  @Test
  public void testMapFreshnessAssertion() {
    // Case 1: Without nullable fields
    AssertionInfo inputInfo = createFreshnessAssertionInfoWithoutNullableFields();
    EntityResponse freshnessAssertionEntityResponse = createAssertionInfoEntityResponse(inputInfo);
    Assertion output = AssertionMapper.map(null, freshnessAssertionEntityResponse);
    verifyAssertionInfo(inputInfo, output);

    // Case 2: With nullable fields
    inputInfo = createDatasetAssertionInfoWithNullableFields();
    EntityResponse freshnessAssertionEntityResponseWithNullables =
        createAssertionInfoEntityResponse(inputInfo);
    output = AssertionMapper.map(null, freshnessAssertionEntityResponseWithNullables);
    verifyAssertionInfo(inputInfo, output);
  }

  @Test
  public void testMapDataSchemaAssertion() {
    AssertionInfo input = createSchemaAssertion();
    EntityResponse schemaAssertionEntityResponse = createAssertionInfoEntityResponse(input);
    Assertion output = AssertionMapper.map(null, schemaAssertionEntityResponse);
    verifyAssertionInfo(input, output);
  }

  @Test
  public void testMapCustomAssertion() {
    // Case 1: Without nullable fields
    AssertionInfo input = createCustomAssertionInfoWithoutNullableFields();
    EntityResponse customAssertionEntityResponse = createAssertionInfoEntityResponse(input);
    Assertion output = AssertionMapper.map(null, customAssertionEntityResponse);
    verifyAssertionInfo(input, output);

    // Case 2: With nullable fields
    input = createCustomAssertionInfoWithNullableFields();
    EntityResponse customAssertionEntityResponseWithNullables =
        createAssertionInfoEntityResponse(input);
    output = AssertionMapper.map(null, customAssertionEntityResponseWithNullables);
    verifyAssertionInfo(input, output);
  }

  private void verifyAssertionInfo(AssertionInfo input, Assertion output) {
    Assert.assertNotNull(output);
    Assert.assertNotNull(output.getInfo());
    Assert.assertEquals(
        output.getInfo().getType().toString(), output.getInfo().getType().toString());

    if (input.hasDatasetAssertion()) {
      verifyDatasetAssertion(input.getDatasetAssertion(), output.getInfo().getDatasetAssertion());
      Assert.assertEquals(
          output.getDataset().getUrn(), input.getDatasetAssertion().getDataset().toString());
    }
    if (input.hasFreshnessAssertion()) {
      verifyFreshnessAssertion(
          input.getFreshnessAssertion(), output.getInfo().getFreshnessAssertion());
      Assert.assertEquals(
          output.getDataset().getUrn(), input.getFreshnessAssertion().getEntity().toString());
    }
    if (input.hasSchemaAssertion()) {
      verifySchemaAssertion(input.getSchemaAssertion(), output.getInfo().getSchemaAssertion());
      Assert.assertEquals(
          output.getDataset().getUrn(), input.getSchemaAssertion().getEntity().toString());
    }
    if (input.hasVolumeAssertion()) {
      verifyVolumeAssertion(input.getVolumeAssertion(), output.getInfo().getVolumeAssertion());
      Assert.assertEquals(
          output.getDataset().getUrn(), input.getCustomAssertion().getEntity().toString());
    }
    if (input.hasSqlAssertion()) {
      verifySqlAssertion(input.getSqlAssertion(), output.getInfo().getSqlAssertion());
      Assert.assertEquals(
          output.getDataset().getUrn(), input.getSqlAssertion().getEntity().toString());
    }
    if (input.hasFieldAssertion()) {
      verifyFieldAssertion(input.getFieldAssertion(), output.getInfo().getFieldAssertion());
      Assert.assertEquals(
          output.getDataset().getUrn(), input.getFieldAssertion().getEntity().toString());
    }
    if (input.hasCustomAssertion()) {
      verifyCustomAssertion(input.getCustomAssertion(), output.getInfo().getCustomAssertion());
      Assert.assertEquals(
          output.getDataset().getUrn(), input.getCustomAssertion().getEntity().toString());
    }

    if (input.hasSource()) {
      verifySource(input.getSource(), output.getInfo().getSource());
    }
    if (input.hasExternalUrl()) {
      Assert.assertEquals(input.getExternalUrl().toString(), output.getInfo().getExternalUrl());
    }
  }

  private void verifyDatasetAssertion(
      DatasetAssertionInfo input,
      com.linkedin.datahub.graphql.generated.DatasetAssertionInfo output) {
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
      Assert.assertTrue(
          input.getFields().stream()
              .allMatch(
                  field ->
                      output.getFields().stream()
                          .anyMatch(outField -> field.toString().equals(outField.getUrn()))));
    }
  }

  private void verifyFreshnessAssertion(
      FreshnessAssertionInfo input,
      com.linkedin.datahub.graphql.generated.FreshnessAssertionInfo output) {
    Assert.assertEquals(output.getType().toString(), input.getType().toString());
    Assert.assertEquals(output.getEntityUrn(), input.getEntity().toString());
    if (input.hasSchedule()) {
      verifyFreshnessSchedule(input.getSchedule(), output.getSchedule());
    }
  }

  private void verifySchemaAssertion(
      SchemaAssertionInfo input,
      com.linkedin.datahub.graphql.generated.SchemaAssertionInfo output) {
    Assert.assertEquals(output.getEntityUrn(), input.getEntity().toString());
    Assert.assertEquals(output.getCompatibility().toString(), input.getCompatibility().toString());
    Assert.assertEquals(
        output.getSchema().getFields().size(), input.getSchema().getFields().size());
  }

  private void verifyVolumeAssertion(
      VolumeAssertionInfo input,
      com.linkedin.datahub.graphql.generated.VolumeAssertionInfo output) {
    // TODO: Complete
    Assert.assertEquals(output.getEntityUrn(), input.getEntity().toString());
    Assert.assertEquals(output.getType().toString(), input.getType().toString());
  }

  private void verifySqlAssertion(
      SqlAssertionInfo input, com.linkedin.datahub.graphql.generated.SqlAssertionInfo output) {
    // TODO: Complete
    Assert.assertEquals(output.getEntityUrn(), input.getEntity().toString());
    Assert.assertEquals(output.getType().toString(), input.getType().toString());
  }

  private void verifyFieldAssertion(
      FieldAssertionInfo input, com.linkedin.datahub.graphql.generated.FieldAssertionInfo output) {
    // TODO: Complete
    Assert.assertEquals(output.getEntityUrn(), input.getEntity().toString());
    Assert.assertEquals(output.getType().toString(), input.getType().toString());
  }

  private void verifyCustomAssertion(
      CustomAssertionInfo input,
      com.linkedin.datahub.graphql.generated.CustomAssertionInfo output) {
    Assert.assertEquals(output.getEntityUrn(), input.getEntity().toString());
    Assert.assertEquals(output.getType(), input.getType());
    if (input.hasLogic()) {
      Assert.assertEquals(output.getLogic(), input.getLogic());
    }
    if (input.hasField()) {
      Assert.assertEquals(output.getField().getPath(), input.getField().getEntityKey().get(1));
    }
  }

  private void verifyCronSchedule(
      FreshnessCronSchedule input,
      com.linkedin.datahub.graphql.generated.FreshnessCronSchedule output) {
    Assert.assertEquals(output.getCron(), input.getCron());
    Assert.assertEquals(output.getTimezone(), input.getTimezone());
    if (input.hasWindowStartOffsetMs()) {
      Assert.assertEquals(output.getWindowStartOffsetMs(), input.getWindowStartOffsetMs());
    }
  }

  private void verifyFreshnessSchedule(
      FreshnessAssertionSchedule input,
      com.linkedin.datahub.graphql.generated.FreshnessAssertionSchedule output) {
    Assert.assertEquals(output.getType().toString(), input.getType().toString());
    if (input.hasCron()) {
      verifyCronSchedule(input.getCron(), output.getCron());
    }
    if (input.hasFixedInterval()) {
      verifyFixedIntervalSchedule(input.getFixedInterval(), output.getFixedInterval());
    }
  }

  private void verifyFixedIntervalSchedule(
      com.linkedin.assertion.FixedIntervalSchedule input, FixedIntervalSchedule output) {
    Assert.assertEquals(output.getMultiple(), (int) input.getMultiple());
    Assert.assertEquals(output.getUnit().toString(), input.getUnit().toString());
  }

  private void verifySource(
      AssertionSource input, com.linkedin.datahub.graphql.generated.AssertionSource output) {
    Assert.assertEquals(output.getType().toString(), input.getType().toString());
  }

  private EntityResponse createAssertionInfoEntityResponse(final AssertionInfo info) {
    HashMap<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(Constants.ASSERTION_INFO_ASPECT_NAME, createEnvelopedAspect(info.data()));
    return createEntityResponse(aspects);
  }

  private EntityResponse createEntityResponse(Map<String, EnvelopedAspect> aspects) {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:assertion:1"));
    entityResponse.setAspects(new EnvelopedAspectMap(new HashMap<>()));
    aspects.forEach(
        (aspectName, envelopedAspect) -> {
          entityResponse.getAspects().put(aspectName, envelopedAspect);
        });

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
    baseInfo.setFields(
        new UrnArray(
            Arrays.asList(
                UrnUtils.getUrn(
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD),field)"))));
    baseInfo.setAggregation(AssertionStdAggregation.SUM);
    baseInfo.setParameters(createAssertionStdParameters());
    baseInfo.setNativeType("native_type");
    baseInfo.setNativeParameters(new StringMap(Collections.singletonMap("key", "value")));
    baseInfo.setLogic("sample_logic");
    infoWithoutNullables.setSource(
        new AssertionSource().setType(com.linkedin.assertion.AssertionSourceType.INFERRED));
    return infoWithoutNullables;
  }

  private AssertionInfo createFreshnessAssertionInfoWithoutNullableFields() {
    AssertionInfo info = new AssertionInfo();
    info.setType(AssertionType.FRESHNESS);
    FreshnessAssertionInfo freshnessAssertionInfo = new FreshnessAssertionInfo();
    freshnessAssertionInfo.setEntity(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)"));
    freshnessAssertionInfo.setType(FreshnessAssertionType.DATASET_CHANGE);
    info.setFreshnessAssertion(freshnessAssertionInfo);
    return info;
  }

  private AssertionInfo createFreshnessAssertionInfoWithNullableFields() {
    AssertionInfo infoWithoutNullables = createFreshnessAssertionInfoWithoutNullableFields();
    FreshnessAssertionInfo baseInfo = infoWithoutNullables.getFreshnessAssertion();
    baseInfo.setSchedule(createFreshnessAssertionSchedule());
    infoWithoutNullables.setSource(
        new AssertionSource().setType(com.linkedin.assertion.AssertionSourceType.INFERRED));
    return infoWithoutNullables;
  }

  private AssertionInfo createSchemaAssertion() {
    AssertionInfo info = new AssertionInfo();
    info.setType(AssertionType.DATA_SCHEMA);
    SchemaAssertionInfo schemaAssertionInfo = new SchemaAssertionInfo();
    schemaAssertionInfo.setEntity(UrnUtils.getUrn("urn:li:dataset:1"));
    schemaAssertionInfo.setCompatibility(SchemaAssertionCompatibility.SUPERSET);
    schemaAssertionInfo.setSchema(
        new SchemaMetadata()
            .setCluster("Test")
            .setHash("Test")
            .setSchemaName("Test")
            .setPlatform(new DataPlatformUrn("mysql"))
            .setPlatformSchema(
                SchemaMetadata.PlatformSchema.create(new MySqlDDL().setTableSchema("")))
            .setVersion(0)
            .setFields(
                new SchemaFieldArray(
                    ImmutableList.of(
                        new SchemaField()
                            .setType(
                                new SchemaFieldDataType()
                                    .setType(SchemaFieldDataType.Type.create(new StringType())))
                            .setNullable(false)
                            .setNativeDataType("string")
                            .setFieldPath("test")))));
    info.setSchemaAssertion(schemaAssertionInfo);
    return info;
  }

  private AssertionInfo createCustomAssertionInfoWithoutNullableFields() {
    AssertionInfo info = new AssertionInfo();
    info.setType(AssertionType.CUSTOM);
    CustomAssertionInfo customAssertionInfo = new CustomAssertionInfo();
    customAssertionInfo.setType("Custom Type 1");
    customAssertionInfo.setEntity(UrnUtils.getUrn("urn:li:dataset:1"));
    info.setCustomAssertion(customAssertionInfo);
    return info;
  }

  private AssertionInfo createCustomAssertionInfoWithNullableFields() {
    AssertionInfo info = new AssertionInfo();
    info.setType(AssertionType.CUSTOM);
    info.setExternalUrl(new Url("https://xyz.com"));
    info.setDescription("Description of custom assertion");
    info.setNote(
        new AssertionNote()
            .setContent("Note of custom assertion")
            .setLastModified(
                new AuditStamp()
                    .setTime(0L)
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:test_user"))));
    CustomAssertionInfo customAssertionInfo = new CustomAssertionInfo();
    customAssertionInfo.setType("Custom Type 1");
    customAssertionInfo.setEntity(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)"));
    customAssertionInfo.setField(
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD),field)"));
    customAssertionInfo.setLogic("custom logic");
    info.setCustomAssertion(customAssertionInfo);
    info.setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL));

    return info;
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
