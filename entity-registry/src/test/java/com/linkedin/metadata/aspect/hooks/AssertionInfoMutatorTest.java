package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.List;
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
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockRetrieverContext.getGraphRetriever()).thenReturn(mockGraphRetriever);
  }

  @Test
  public void testIncorrectAspect() {
    assertEquals(
        test.writeMutation(
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
    assertEquals(mutatedInfo.getEntity(), testDatasetUrn);
  }

  @Test
  public void testDatasetAssertionWithEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo().setDataset(testDatasetUrn))
            .setEntity(testDatasetUrn);

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
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
    assertEquals(mutatedInfo.getEntity(), testEntityUrn);
  }

  @Test
  public void testVolumeAssertionWithoutEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.VOLUME)
            .setVolumeAssertion(new VolumeAssertionInfo().setEntity(testEntityUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
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
    assertEquals(mutatedInfo.getEntity(), testEntityUrn);
  }

  @Test
  public void testSqlAssertionWithoutEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.SQL)
            .setSqlAssertion(new SqlAssertionInfo().setEntity(testEntityUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
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
    assertEquals(mutatedInfo.getEntity(), testEntityUrn);
  }

  @Test
  public void testFieldAssertionWithoutEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.FIELD)
            .setFieldAssertion(new FieldAssertionInfo().setEntity(testEntityUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
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
    assertEquals(mutatedInfo.getEntity(), testEntityUrn);
  }

  @Test
  public void testSchemaAssertionWithoutEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATA_SCHEMA)
            .setSchemaAssertion(new SchemaAssertionInfo().setEntity(testEntityUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
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
    assertEquals(mutatedInfo.getEntity(), testEntityUrn);
  }

  @Test
  public void testCustomAssertionWithoutEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.CUSTOM)
            .setCustomAssertion(new CustomAssertionInfo().setEntity(testEntityUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
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
    assertEquals(mutatedInfo.getEntity(), testEntityUrn);
  }

  @Test
  public void testPatchChangeType() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo().setDataset(testDatasetUrn));

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
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
    assertEquals(mutatedInfo.getEntity(), testDatasetUrn);
  }

  @Test
  public void testPatchChangeTypeWithExistingEntity() {
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo().setDataset(testDatasetUrn))
            .setEntity(testDatasetUrn);

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
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
}
