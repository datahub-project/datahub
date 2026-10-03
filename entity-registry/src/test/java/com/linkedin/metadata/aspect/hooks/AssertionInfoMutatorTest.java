package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.context.OperationFingerprint;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FieldAssertionType;
import com.linkedin.assertion.FieldMetricAssertion;
import com.linkedin.assertion.FieldMetricType;
import com.linkedin.assertion.FieldValuesAssertion;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.schema.SchemaFieldSpec;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class AssertionInfoMutatorTest {

  private EntityRegistry entityRegistry;
  private RetrieverContext mockRetrieverContext;
  private Urn testAssertionUrn;
  private Urn testDatasetUrn;
  private Urn testEntityUrn;
  private OperationFingerprint operationFingerprint;
  private final AssertionInfoMutator test =
      new AssertionInfoMutator().setConfig(mock(AspectPluginConfig.class));

  @BeforeTest
  public void init() throws URISyntaxException {
    testAssertionUrn = UrnUtils.getUrn("urn:li:assertion:test-assertion");
    testDatasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test.dataset,PROD)");
    testEntityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)");

    entityRegistry = new TestEntityRegistry();
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    GraphRetriever mockGraphRetriever = mock(GraphRetriever.class);
    mockRetrieverContext = mock(RetrieverContext.class);
    operationFingerprint = OperationFingerprint.EMPTY;
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockRetrieverContext.getGraphRetriever()).thenReturn(mockGraphRetriever);
  }

  @Test
  public void testIncorrectAspect() {
    assertEquals(
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testDatasetUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testDatasetUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testDatasetUrn.getEntityType())
                                .getAspectSpec(GLOBAL_TAGS_ASPECT_NAME))
                        .recordTemplate(null)
                        .build()),
                mockRetrieverContext)
            .filter(Pair::getSecond)
            .count(),
        0);
  }

  @Test
  public void testDeleteChangeType() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo().setDataset(testDatasetUrn));

    assertEquals(
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .filter(Pair::getSecond)
            .count(),
        0);
  }

  @Test
  public void testDatasetAssertionWithoutEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo().setDataset(testDatasetUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 1);
    AssertionInfo mutatedInfo = result.get(0).getFirst().getAspect(AssertionInfo.class);
    assertEquals(mutatedInfo.getEntityUrn(), testDatasetUrn);
  }

  @Test
  public void testDatasetAssertionWithEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo().setDataset(testDatasetUrn))
            .setEntityUrn(testDatasetUrn);

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 0);
  }

  @Test
  public void testFreshnessAssertionWithoutEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.FRESHNESS)
            .setFreshnessAssertion(new FreshnessAssertionInfo().setEntity(testEntityUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 1);
    AssertionInfo mutatedInfo = result.get(0).getFirst().getAspect(AssertionInfo.class);
    assertEquals(mutatedInfo.getEntityUrn(), testEntityUrn);
  }

  @Test
  public void testVolumeAssertionWithoutEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.VOLUME)
            .setVolumeAssertion(new VolumeAssertionInfo().setEntity(testEntityUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 1);
    AssertionInfo mutatedInfo = result.get(0).getFirst().getAspect(AssertionInfo.class);
    assertEquals(mutatedInfo.getEntityUrn(), testEntityUrn);
  }

  @Test
  public void testSqlAssertionWithoutEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.SQL)
            .setSqlAssertion(new SqlAssertionInfo().setEntity(testEntityUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 1);
    AssertionInfo mutatedInfo = result.get(0).getFirst().getAspect(AssertionInfo.class);
    assertEquals(mutatedInfo.getEntityUrn(), testEntityUrn);
  }

  @Test
  public void testFieldAssertionWithoutEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.FIELD)
            .setFieldAssertion(new FieldAssertionInfo().setEntity(testEntityUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 1);
    AssertionInfo mutatedInfo = result.get(0).getFirst().getAspect(AssertionInfo.class);
    assertEquals(mutatedInfo.getEntityUrn(), testEntityUrn);
  }

  @Test
  public void testSchemaAssertionWithoutEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATA_SCHEMA)
            .setSchemaAssertion(new SchemaAssertionInfo().setEntity(testEntityUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 1);
    AssertionInfo mutatedInfo = result.get(0).getFirst().getAspect(AssertionInfo.class);
    assertEquals(mutatedInfo.getEntityUrn(), testEntityUrn);
  }

  @Test
  public void testCustomAssertionWithoutEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.CUSTOM)
            .setCustomAssertion(new CustomAssertionInfo().setEntity(testEntityUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 1);
    AssertionInfo mutatedInfo = result.get(0).getFirst().getAspect(AssertionInfo.class);
    assertEquals(mutatedInfo.getEntityUrn(), testEntityUrn);
  }

  @Test
  public void testPatchChangeType() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo().setDataset(testDatasetUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.PATCH)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 1);
    AssertionInfo mutatedInfo = result.get(0).getFirst().getAspect(AssertionInfo.class);
    assertEquals(mutatedInfo.getEntityUrn(), testDatasetUrn);
  }

  @Test
  public void testPatchChangeTypeWithExistingEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo().setDataset(testDatasetUrn))
            .setEntityUrn(testDatasetUrn);

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.PATCH)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 0);
  }

  @Test
  public void testNullNestedAssertionDoesNotThrow() {
    AssertionInfo assertionInfo = new AssertionInfo().setType(AssertionType.DATASET);

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 0);
    AssertionInfo mutatedInfo = result.get(0).getFirst().getAspect(AssertionInfo.class);
    assertNotNull(mutatedInfo);
    assertNull(mutatedInfo.getEntityUrn());
  }

  @Test
  public void testBatchWithOneInvalidAssertionMutatesOthers() throws Exception {
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:test-assertion-1");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:test-assertion-2");
    Urn assertionUrn3 = UrnUtils.getUrn("urn:li:assertion:test-assertion-3");

    AssertionInfo validDataset =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo().setDataset(testDatasetUrn));

    // Invalid: type says DATASET, but the nested payload is missing.
    AssertionInfo invalidDatasetMissingPayload = new AssertionInfo().setType(AssertionType.DATASET);

    AssertionInfo validFreshness =
        new AssertionInfo()
            .setType(AssertionType.FRESHNESS)
            .setFreshnessAssertion(new FreshnessAssertionInfo().setEntity(testEntityUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(assertionUrn1)
                        .entitySpec(entityRegistry.getEntitySpec(assertionUrn1.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(assertionUrn1.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(validDataset)
                        .build(),
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(assertionUrn2)
                        .entitySpec(entityRegistry.getEntitySpec(assertionUrn2.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(assertionUrn2.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(invalidDatasetMissingPayload)
                        .build(),
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(assertionUrn3)
                        .entitySpec(entityRegistry.getEntitySpec(assertionUrn3.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(assertionUrn3.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(validFreshness)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.size(), 3);

    Map<Urn, Pair<ChangeMCP, Boolean>> byUrn =
        result.stream().collect(Collectors.toMap(p -> p.getFirst().getUrn(), p -> p));

    assertTrue(byUrn.get(assertionUrn1).getSecond());
    assertEquals(
        byUrn.get(assertionUrn1).getFirst().getAspect(AssertionInfo.class).getEntityUrn(),
        testDatasetUrn);

    assertFalse(byUrn.get(assertionUrn2).getSecond());
    assertNull(byUrn.get(assertionUrn2).getFirst().getAspect(AssertionInfo.class).getEntityUrn());

    assertTrue(byUrn.get(assertionUrn3).getSecond());
    assertEquals(
        byUrn.get(assertionUrn3).getFirst().getAspect(AssertionInfo.class).getEntityUrn(),
        testEntityUrn);
  }

  // ==================== fieldPath Mutation Tests ====================

  @Test
  public void testFieldAssertionWithFieldMetricPopulatesFieldPath() {
    SchemaFieldSpec fieldSpec = new SchemaFieldSpec();
    fieldSpec.setPath("col1");
    fieldSpec.setType("STRING");
    fieldSpec.setNativeType("VARCHAR");

    FieldMetricAssertion fieldMetricAssertion = new FieldMetricAssertion();
    fieldMetricAssertion.setField(fieldSpec);
    fieldMetricAssertion.setMetric(FieldMetricType.NULL_COUNT);
    fieldMetricAssertion.setOperator(com.linkedin.assertion.AssertionStdOperator.BETWEEN);

    FieldAssertionInfo fieldInfo = new FieldAssertionInfo();
    fieldInfo.setType(FieldAssertionType.FIELD_METRIC);
    fieldInfo.setEntity(testEntityUrn);
    fieldInfo.setFieldMetricAssertion(fieldMetricAssertion);

    AssertionInfo assertionInfo =
        new AssertionInfo().setType(AssertionType.FIELD).setFieldAssertion(fieldInfo);

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 1);
    AssertionInfo mutatedInfo = result.get(0).getFirst().getAspect(AssertionInfo.class);
    assertEquals(mutatedInfo.getFieldAssertion().getFieldPath(), "col1");
  }

  @Test
  public void testFieldAssertionWithFieldValuesPopulatesFieldPath() {
    SchemaFieldSpec fieldSpec = new SchemaFieldSpec();
    fieldSpec.setPath("col2");
    fieldSpec.setType("STRING");
    fieldSpec.setNativeType("VARCHAR");

    FieldValuesAssertion fieldValuesAssertion = new FieldValuesAssertion();
    fieldValuesAssertion.setField(fieldSpec);
    fieldValuesAssertion.setOperator(com.linkedin.assertion.AssertionStdOperator.BETWEEN);

    FieldAssertionInfo fieldInfo = new FieldAssertionInfo();
    fieldInfo.setType(FieldAssertionType.FIELD_VALUES);
    fieldInfo.setEntity(testEntityUrn);
    fieldInfo.setFieldValuesAssertion(fieldValuesAssertion);

    AssertionInfo assertionInfo =
        new AssertionInfo().setType(AssertionType.FIELD).setFieldAssertion(fieldInfo);

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 1);
    AssertionInfo mutatedInfo = result.get(0).getFirst().getAspect(AssertionInfo.class);
    assertEquals(mutatedInfo.getFieldAssertion().getFieldPath(), "col2");
  }

  @Test
  public void testFieldAssertionWithExistingFieldPathNotOverwritten() {
    SchemaFieldSpec fieldSpec = new SchemaFieldSpec();
    fieldSpec.setPath("col1");
    fieldSpec.setType("STRING");
    fieldSpec.setNativeType("VARCHAR");

    FieldMetricAssertion fieldMetricAssertion = new FieldMetricAssertion();
    fieldMetricAssertion.setField(fieldSpec);
    fieldMetricAssertion.setMetric(FieldMetricType.NULL_COUNT);
    fieldMetricAssertion.setOperator(com.linkedin.assertion.AssertionStdOperator.BETWEEN);

    FieldAssertionInfo fieldInfo = new FieldAssertionInfo();
    fieldInfo.setType(FieldAssertionType.FIELD_METRIC);
    fieldInfo.setEntity(testEntityUrn);
    fieldInfo.setFieldMetricAssertion(fieldMetricAssertion);
    fieldInfo.setFieldPath("existing_col");

    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.FIELD)
            .setFieldAssertion(fieldInfo)
            .setEntityUrn(testEntityUrn);

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 1);
    AssertionInfo mutatedInfo = result.get(0).getFirst().getAspect(AssertionInfo.class);
    assertEquals(mutatedInfo.getFieldAssertion().getFieldPath(), "existing_col");
    assertEquals(mutatedInfo.getFieldPaths(), new StringArray("col1"));
  }

  @Test
  public void testNonFieldAssertionDoesNotSetFieldPath() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.FRESHNESS)
            .setFreshnessAssertion(new FreshnessAssertionInfo().setEntity(testEntityUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    AssertionInfo mutatedInfo = result.get(0).getFirst().getAspect(AssertionInfo.class);
    assertFalse(mutatedInfo.hasFieldAssertion());
  }

  @Test
  public void testFieldAssertionWithNoNestedFieldPathHandledGracefully() {
    FieldAssertionInfo fieldInfo = new FieldAssertionInfo();
    fieldInfo.setType(FieldAssertionType.FIELD_METRIC);
    fieldInfo.setEntity(testEntityUrn);

    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.FIELD)
            .setFieldAssertion(fieldInfo)
            .setEntityUrn(testEntityUrn);

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                operationFingerprint,
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testAssertionUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testAssertionUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testAssertionUrn.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 0);
    AssertionInfo mutatedInfo = result.get(0).getFirst().getAspect(AssertionInfo.class);
    assertFalse(mutatedInfo.getFieldAssertion().hasFieldPath());
  }

  @Test
  public void testCustomFieldPathsFollowAssociations() {
    String nestedPath = "[version=2.0].[type=struct].address.[type=string].city";
    Urn first = Urn.createFromTuple("schemaField", testDatasetUrn.toString(), nestedPath);
    Urn second = Urn.createFromTuple("schemaField", testDatasetUrn.toString(), "col_b");
    CustomAssertionInfo custom =
        new CustomAssertionInfo()
            .setType("Quality")
            .setEntity(testDatasetUrn)
            .setField(second)
            .setFields(new UrnArray(first, second, first));
    AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.CUSTOM)
            .setEntityUrn(testDatasetUrn)
            .setCustomAssertion(custom);
    info.data().put("fieldPaths", new StringArray("stale").data());

    assertTrue(mutateAssertion(info));
    assertEquals(info.data().get("fieldPaths"), new StringArray(nestedPath, "col_b").data());
    assertFalse(mutateAssertion(info));

    custom.setFields(new UrnArray(second));
    assertTrue(mutateAssertion(info));
    assertEquals(info.data().get("fieldPaths"), new StringArray("col_b").data());

    custom.setFields(new UrnArray());
    assertTrue(mutateAssertion(info));
    assertEquals(info.getFieldPaths(), new StringArray());
    custom.removeFields();
    assertTrue(mutateAssertion(info));
    assertEquals(info.getFieldPaths(), new StringArray("col_b"));
    custom.removeField();
    assertTrue(mutateAssertion(info));
    assertEquals(info.data().get("fieldPaths"), new StringArray().data());
  }

  @Test
  public void testCustomLegacyFieldAndDatasetFields() {
    String path = "func(a,b)+50%off%20";
    Urn field = SchemaFieldUtils.generateSchemaFieldUrn(testDatasetUrn, path);
    CustomAssertionInfo custom =
        new CustomAssertionInfo().setType("Quality").setEntity(testDatasetUrn).setField(field);
    AssertionInfo info =
        new AssertionInfo().setType(AssertionType.CUSTOM).setCustomAssertion(custom);
    mutateAssertion(info);
    assertEquals(info.data().get("fieldPaths"), new StringArray(path).data());

    DatasetAssertionInfo dataset =
        new DatasetAssertionInfo().setDataset(testDatasetUrn).setFields(new UrnArray(field));
    info = new AssertionInfo().setType(AssertionType.DATASET).setDatasetAssertion(dataset);
    mutateAssertion(info);
    assertEquals(info.data().get("fieldPaths"), new StringArray(path).data());
    dataset.removeFields();
    mutateAssertion(info);
    assertEquals(info.data().get("fieldPaths"), new StringArray().data());
  }

  @Test
  public void testPartialAssertionAndMalformedFieldAssociations() {
    AssertionInfo partial = new AssertionInfo().setFieldPaths(new StringArray("stale"));
    assertTrue(mutateAssertion(partial));
    assertTrue(partial.getFieldPaths().isEmpty());

    AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.CUSTOM)
            .setCustomAssertion(
                new CustomAssertionInfo()
                    .setType("Quality")
                    .setEntity(testDatasetUrn)
                    .setFields(
                        new UrnArray(
                            UrnUtils.getUrn("urn:li:schemaField:malformed"),
                            SchemaFieldUtils.generateSchemaFieldUrn(testDatasetUrn, "col_a"))));
    assertTrue(mutateAssertion(info));
    assertEquals(info.getFieldPaths(), new StringArray("col_a"));
  }

  private boolean mutateAssertion(AssertionInfo info) {
    return test.writeMutation(
            operationFingerprint,
            List.of(
                TestMCP.builder()
                    .changeType(ChangeType.UPSERT)
                    .urn(testAssertionUrn)
                    .entitySpec(entityRegistry.getEntitySpec(ASSERTION_ENTITY_NAME))
                    .aspectSpec(
                        entityRegistry
                            .getEntitySpec(ASSERTION_ENTITY_NAME)
                            .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                    .recordTemplate(info)
                    .build()),
            mockRetrieverContext)
        .findFirst()
        .orElseThrow()
        .getSecond();
  }
}
