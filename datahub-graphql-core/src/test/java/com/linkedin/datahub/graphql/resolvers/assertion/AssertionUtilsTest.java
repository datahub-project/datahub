package com.linkedin.datahub.graphql.resolvers.assertion;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FieldAssertionType;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.SqlAssertionType;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.assertion.VolumeAssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AssertionActionInput;
import com.linkedin.datahub.graphql.generated.AssertionActionType;
import com.linkedin.datahub.graphql.generated.AssertionActionsInput;
import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterInput;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterType;
import com.linkedin.datahub.graphql.generated.AssertionStdParametersInput;
import com.linkedin.datahub.graphql.generated.CreateFieldAssertionInput;
import com.linkedin.datahub.graphql.generated.CreateFreshnessAssertionInput;
import com.linkedin.datahub.graphql.generated.CreateSqlAssertionInput;
import com.linkedin.datahub.graphql.generated.CreateVolumeAssertionInput;
import com.linkedin.datahub.graphql.generated.DatasetFilterInput;
import com.linkedin.datahub.graphql.generated.DatasetFilterType;
import com.linkedin.datahub.graphql.generated.RunAssertionResult;
import com.linkedin.datahub.graphql.generated.RunAssertionsResult;
import com.linkedin.datahub.graphql.generated.SchemaFieldSpecInput;
import com.linkedin.datahub.graphql.generated.TestAssertionInput;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AssertionUtilsTest {

  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
  private static final Urn TEST_DATAJOB_URN = UrnUtils.getUrn("urn:li:dataJob:test");
  private static final AssertionStdParameterInput TEST_PARAMETER_INPUT =
      new AssertionStdParameterInput();
  private static final AssertionStdParametersInput TEST_PARAMETERS_INPUT =
      new AssertionStdParametersInput();
  private static final AssertionStdOperator TEST_OPERATOR = AssertionStdOperator.IN;
  private static final SchemaFieldSpecInput TEST_SCHEMA_FIELD_SPEC_INPUT =
      new SchemaFieldSpecInput();
  private static final DatasetFilterInput TEST_DATASET_FILTER_INPUT = new DatasetFilterInput();
  private static final AssertionActionsInput TEST_ASSERTION_ACTIONS_INPUT =
      new AssertionActionsInput();
  private static final AssertionActionInput TEST_ASSERTION_ACTION_SUCCESS_INPUT =
      new AssertionActionInput();
  private static final AssertionActionInput TEST_ASSERTION_ACTION_FAILURE_INPUT =
      new AssertionActionInput();
  private static final List<AssertionActionInput> TEST_ASSERTION_ACTION_SUCCESS_LIST_INPUT =
      new ArrayList<AssertionActionInput>();
  private static final List<AssertionActionInput> TEST_ASSERTION_ACTION_FAILURE_LIST_INPUT =
      new ArrayList<AssertionActionInput>();

  @BeforeMethod
  void setUp() {
    // Set up test parameter input
    TEST_PARAMETER_INPUT.setType(AssertionStdParameterType.NUMBER);
    TEST_PARAMETER_INPUT.setValue("10");

    // Set up test parameters input
    TEST_PARAMETERS_INPUT.setValue(TEST_PARAMETER_INPUT);

    // Set up test schema field spec input
    TEST_SCHEMA_FIELD_SPEC_INPUT.setType("STRING");
    TEST_SCHEMA_FIELD_SPEC_INPUT.setNativeType("VARCHAR");
    TEST_SCHEMA_FIELD_SPEC_INPUT.setPath("test");

    // Set up test dataset filter input
    TEST_DATASET_FILTER_INPUT.setType(DatasetFilterType.SQL);
    TEST_DATASET_FILTER_INPUT.setSql("WHERE value > 10");

    // Set up test assertion actions input
    TEST_ASSERTION_ACTION_SUCCESS_INPUT.setType(AssertionActionType.RESOLVE_INCIDENT);
    TEST_ASSERTION_ACTION_FAILURE_INPUT.setType(AssertionActionType.RAISE_INCIDENT);
    TEST_ASSERTION_ACTION_SUCCESS_LIST_INPUT.add(TEST_ASSERTION_ACTION_SUCCESS_INPUT);
    TEST_ASSERTION_ACTION_FAILURE_LIST_INPUT.add(TEST_ASSERTION_ACTION_FAILURE_INPUT);
    TEST_ASSERTION_ACTIONS_INPUT.setOnSuccess(TEST_ASSERTION_ACTION_SUCCESS_LIST_INPUT);
    TEST_ASSERTION_ACTIONS_INPUT.setOnFailure(TEST_ASSERTION_ACTION_FAILURE_LIST_INPUT);
  }

  @Test
  public void testCreateDatasetAssertionParameters() {
    final com.linkedin.assertion.AssertionStdParameters result =
        AssertionUtils.createDatasetAssertionParameters(TEST_PARAMETERS_INPUT);
    Assert.assertEquals(
        result.getValue().getType(), com.linkedin.assertion.AssertionStdParameterType.NUMBER);
    Assert.assertEquals(result.getValue().getValue(), "10");
  }

  @Test
  public void testCreateDatasetAssertionParameter() {
    final com.linkedin.assertion.AssertionStdParameter result =
        AssertionUtils.createDatasetAssertionParameter(TEST_PARAMETER_INPUT);
    Assert.assertEquals(result.getType(), com.linkedin.assertion.AssertionStdParameterType.NUMBER);
    Assert.assertEquals(result.getValue(), "10");
  }

  @Test
  public void testCreateAssertionStdOperator() {
    final com.linkedin.assertion.AssertionStdOperator result =
        AssertionUtils.createAssertionStdOperator(TEST_OPERATOR);
    Assert.assertEquals(result, com.linkedin.assertion.AssertionStdOperator.IN);
  }

  @Test
  public void testCreateSchemaFieldSpec() {
    final com.linkedin.schema.SchemaFieldSpec result =
        AssertionUtils.createSchemaFieldSpec(TEST_SCHEMA_FIELD_SPEC_INPUT);
    Assert.assertEquals(result.getType(), "STRING");
    Assert.assertEquals(result.getNativeType(), "VARCHAR");
    Assert.assertEquals(result.getPath(), "test");
  }

  @Test
  public void testCreateAssertionFilter() {
    final com.linkedin.dataset.DatasetFilter result =
        AssertionUtils.createAssertionFilter(TEST_DATASET_FILTER_INPUT);
    Assert.assertEquals(result.getType(), com.linkedin.dataset.DatasetFilterType.SQL);
    Assert.assertEquals(result.getSql(), "WHERE value > 10");
  }

  @Test
  public void testCreateAssertionActions() {
    final com.linkedin.assertion.AssertionActions result =
        AssertionUtils.createAssertionActions(TEST_ASSERTION_ACTIONS_INPUT);
    Assert.assertEquals(
        result.getOnSuccess().get(0).getType(),
        com.linkedin.assertion.AssertionActionType.RESOLVE_INCIDENT);
    Assert.assertEquals(
        result.getOnFailure().get(0).getType(),
        com.linkedin.assertion.AssertionActionType.RAISE_INCIDENT);
  }

  @Test
  public void testGetAsserteeUrnFromInfo() {
    // Case 1: Dataset Assertion
    AssertionInfo info = new AssertionInfo();
    info.setType(AssertionType.DATASET);
    info.setDatasetAssertion(
        new DatasetAssertionInfo()
            .setOperator(com.linkedin.assertion.AssertionStdOperator.IN)
            .setScope(DatasetAssertionScope.DATASET_COLUMN)
            .setDataset(TEST_DATASET_URN)
            .setAggregation(AssertionStdAggregation.MAX));
    Urn result = AssertionUtils.getAsserteeUrnFromInfo(info);
    Assert.assertEquals(result, TEST_DATASET_URN);

    // Case 2: Dataset FRESHNESS Assertion
    info = new AssertionInfo();
    info.setType(AssertionType.FRESHNESS);
    info.setFreshnessAssertion(
        new FreshnessAssertionInfo()
            .setType(FreshnessAssertionType.DATASET_CHANGE)
            .setEntity(TEST_DATASET_URN));
    result = AssertionUtils.getAsserteeUrnFromInfo(info);
    Assert.assertEquals(result, TEST_DATASET_URN);

    // Case 3: DataJob FRESHNESS Assertion
    info = new AssertionInfo();
    info.setType(AssertionType.FRESHNESS);
    info.setFreshnessAssertion(
        new FreshnessAssertionInfo()
            .setType(FreshnessAssertionType.DATASET_CHANGE)
            .setEntity(TEST_DATAJOB_URN));
    result = AssertionUtils.getAsserteeUrnFromInfo(info);
    Assert.assertEquals(result, TEST_DATAJOB_URN);

    // Case 4: VOLUME Assertion
    info = new AssertionInfo();
    info.setType(AssertionType.VOLUME);
    info.setVolumeAssertion(
        new VolumeAssertionInfo()
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL)
            .setEntity(TEST_DATASET_URN));
    result = AssertionUtils.getAsserteeUrnFromInfo(info);
    Assert.assertEquals(result, TEST_DATASET_URN);

    // Case 5: SQL Assertion
    info = new AssertionInfo();
    info.setType(AssertionType.SQL);
    info.setSqlAssertion(
        new SqlAssertionInfo().setType(SqlAssertionType.METRIC).setEntity(TEST_DATASET_URN));
    result = AssertionUtils.getAsserteeUrnFromInfo(info);
    Assert.assertEquals(result, TEST_DATASET_URN);
    Assert.assertEquals(result, TEST_DATASET_URN);

    // Case 6: FIELD Assertion
    info = new AssertionInfo();
    info.setType(AssertionType.FIELD);
    info.setFieldAssertion(
        new FieldAssertionInfo()
            .setType(FieldAssertionType.FIELD_VALUES)
            .setEntity(TEST_DATASET_URN));
    result = AssertionUtils.getAsserteeUrnFromInfo(info);
    Assert.assertEquals(result, TEST_DATASET_URN);

    // Case 7: Unsupported Assertion Type
    final AssertionInfo badInfo = new AssertionInfo();
    info.setType(AssertionType.$UNKNOWN);
    info.setFreshnessAssertion(
        new FreshnessAssertionInfo()
            .setType(FreshnessAssertionType.DATA_JOB_RUN)
            .setEntity(TEST_DATASET_URN));
    Assert.assertThrows(
        RuntimeException.class, () -> AssertionUtils.getAsserteeUrnFromInfo(badInfo));
  }

  @Test
  public void testGetAsserteeUrnFromTestInput() {
    TestAssertionInput testAssertonInput = new TestAssertionInput();

    // Case 1: FRESHNESS Assertion
    CreateFreshnessAssertionInput freshnessAssertionInput = new CreateFreshnessAssertionInput();
    freshnessAssertionInput.setEntityUrn(TEST_DATASET_URN.toString());
    testAssertonInput.setType(com.linkedin.datahub.graphql.generated.AssertionType.FRESHNESS);
    testAssertonInput.setFreshnessTestInput(freshnessAssertionInput);
    String result = AssertionUtils.getAsserteeUrnFromTestInput(testAssertonInput);
    Assert.assertEquals(result, TEST_DATASET_URN.toString());

    // Case 2: VOLUME Assertion
    CreateVolumeAssertionInput volumeAssertionInput = new CreateVolumeAssertionInput();
    volumeAssertionInput.setEntityUrn(TEST_DATASET_URN.toString());
    testAssertonInput.setType(com.linkedin.datahub.graphql.generated.AssertionType.VOLUME);
    testAssertonInput.setVolumeTestInput(volumeAssertionInput);
    result = AssertionUtils.getAsserteeUrnFromTestInput(testAssertonInput);
    Assert.assertEquals(result, TEST_DATASET_URN.toString());

    // Case 3: SQL Assertion
    CreateSqlAssertionInput sqlAssertionInput = new CreateSqlAssertionInput();
    sqlAssertionInput.setEntityUrn(TEST_DATASET_URN.toString());
    testAssertonInput.setType(com.linkedin.datahub.graphql.generated.AssertionType.SQL);
    testAssertonInput.setSqlTestInput(sqlAssertionInput);
    result = AssertionUtils.getAsserteeUrnFromTestInput(testAssertonInput);
    Assert.assertEquals(result, TEST_DATASET_URN.toString());

    // Case 4: FIELD Assertion
    CreateFieldAssertionInput fieldAssertionInput = new CreateFieldAssertionInput();
    fieldAssertionInput.setEntityUrn(TEST_DATASET_URN.toString());
    testAssertonInput.setType(com.linkedin.datahub.graphql.generated.AssertionType.FIELD);
    testAssertonInput.setFieldTestInput(fieldAssertionInput);
    result = AssertionUtils.getAsserteeUrnFromTestInput(testAssertonInput);
    Assert.assertEquals(result, TEST_DATASET_URN.toString());
  }

  @Test
  public void testValidateAssertionSource() {
    // Case 1: External Assertion
    final AssertionInfo info1 = new AssertionInfo();
    info1.setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> AssertionUtils.validateAssertionSource(TEST_ASSERTION_URN, info1));

    // Case 2: Null Assertion
    final AssertionInfo info2 = new AssertionInfo();
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> AssertionUtils.validateAssertionSource(TEST_ASSERTION_URN, info2));

    // Case 3: Native Assertion
    final AssertionInfo info3 = new AssertionInfo();
    info3.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));
    // No exception throw
    AssertionUtils.validateAssertionSource(TEST_ASSERTION_URN, info3);
  }

  @Test
  public void testExtractRunResults() {
    Urn testUrn = UrnUtils.getUrn("urn:li:assertion:test");
    Urn testUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");
    Urn testUrn3 = UrnUtils.getUrn("urn:li:assertion:test3");

    Set<String> validUrns =
        ImmutableSet.of(testUrn.toString(), testUrn2.toString(), testUrn3.toString());

    final QueryContext context = Mockito.mock(QueryContext.class);
    final List<Urn> assertionUrns = ImmutableList.of(testUrn, testUrn2, testUrn3);
    final Map<Urn, AssertionResult> results =
        ImmutableMap.of(
            testUrn,
            new AssertionResult()
                .setType(AssertionResultType.SUCCESS)
                .setRowCount(10)
                .setActualAggValue(20)
                .setMissingCount(5)
                .setUnexpectedCount(3)
                .setExternalUrl("test")
                .setAssertion(new AssertionInfo().setType(AssertionType.DATASET))
                .setNativeResults(new StringMap()),
            testUrn2,
            new AssertionResult()
                .setType(AssertionResultType.FAILURE)
                .setRowCount(10)
                .setActualAggValue(20)
                .setMissingCount(5)
                .setUnexpectedCount(3)
                .setExternalUrl("test")
                .setAssertion(new AssertionInfo().setType(AssertionType.DATASET))
                .setNativeResults(new StringMap()),
            testUrn3,
            new AssertionResult()
                .setType(AssertionResultType.ERROR)
                .setRowCount(10)
                .setActualAggValue(20)
                .setMissingCount(5)
                .setUnexpectedCount(3)
                .setExternalUrl("test")
                .setAssertion(new AssertionInfo().setType(AssertionType.DATASET))
                .setNativeResults(new StringMap()));
    RunAssertionsResult result = AssertionUtils.extractRunResults(context, assertionUrns, results);
    Assert.assertEquals(result.getResults().size(), 3);
    Assert.assertEquals(result.getErrorCount(), 1);
    Assert.assertEquals(result.getFailingCount(), 1);
    Assert.assertEquals(result.getPassingCount(), 1);

    for (RunAssertionResult res : result.getResults()) {
      Assert.assertTrue(validUrns.contains(res.getAssertion().getUrn()));
    }
  }
}
