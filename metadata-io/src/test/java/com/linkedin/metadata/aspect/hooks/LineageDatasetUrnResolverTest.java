package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.*;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.FineGrainedLineageDownstreamType;
import com.linkedin.dataset.FineGrainedLineageUpstreamType;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.platformresource.PlatformResourceInfo;
import com.linkedin.schema.*;
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

public class LineageDatasetUrnResolverTest {

  private EntityRegistry entityRegistry;
  private AspectRetriever mockAspectRetriever;
  private RetrieverContext mockRetrieverContext;
  private Urn upstreamIcebergUrn =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:iceberg,fooWh.fooNs.fooTable,PROD)");
  private Urn upstreamIcebergUrnResolved =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:iceberg,fooTableUUID,PROD)");
  private Urn upstreamResourceUrn =
      UrnUtils.getUrn("urn:li:platformResource:iceberg.fooWh.fooNs.fooTable");
  private PlatformResourceInfo upstreamResourceInfo =
      new PlatformResourceInfo().setPrimaryKey(upstreamIcebergUrnResolved.toString());
  private Urn upstreamNonExistentUrn =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:iceberg,nonExistent,PROD)");
  private Urn upstreamNonExistentResourceUrn =
      UrnUtils.getUrn("urn:li:platformResource:iceberg.nonExistent");
  private Urn upstreamExistingUrnNoResolution =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:iceberg,existing,PROD)");
  private Urn upstreamIcebergSchemaFieldUrn =
      UrnUtils.getUrn(String.format("urn:li:schemaField:(%s,%s)", upstreamIcebergUrn, "foo_id"));
  private Urn upstreamIcebergSchemaFieldUrnResolved =
      UrnUtils.getUrn(
          String.format("urn:li:schemaField:(%s,%s)", upstreamIcebergUrnResolved, "foo_id"));
  private Urn upstreamExistingSchemaFieldUrnNoResolution =
      UrnUtils.getUrn(
          String.format("urn:li:schemaField:(%s,%s)", upstreamExistingUrnNoResolution, "foo_id"));

  private Urn downstreamIcebergUrn =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:iceberg,fooWh.fooNs.barTable,PROD)");
  private Urn downstreamIcebergUrnResolved =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:iceberg,barTableUUID,PROD)");
  private Urn downstreamResourceUrn =
      UrnUtils.getUrn("urn:li:platformResource:iceberg.fooWh.fooNs.barTable");
  private PlatformResourceInfo downstreamResourceInfo =
      new PlatformResourceInfo().setPrimaryKey(downstreamIcebergUrnResolved.toString());
  private Urn downstreamIcebergSchemaFieldUrn =
      UrnUtils.getUrn(String.format("urn:li:schemaField:(%s,%s)", downstreamIcebergUrn, "foo_id"));
  private Urn downstreamIcebergSchemaFieldUrnResolved =
      UrnUtils.getUrn(
          String.format("urn:li:schemaField:(%s,%s)", downstreamIcebergUrnResolved, "foo_id"));

  private Urn upstreamUrnOtherPlatform =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,somedb.someTable1,PROD)");
  private Urn downstreamUrnOtherPlatform =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,somedb.someTable2,PROD)");

  private Urn jobUrn =
      UrnUtils.getUrn("urn:li:dataJob:(urn:li:dataFlow:(spark,TestFlow,prod),Task1)");

  @BeforeTest
  public void init() throws URISyntaxException {
    entityRegistry = new TestEntityRegistry();
    mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    GraphRetriever mockGraphRetriever = mock(GraphRetriever.class);
    mockRetrieverContext = mock(RetrieverContext.class);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockRetrieverContext.getGraphRetriever()).thenReturn(mockGraphRetriever);
  }

  @Test
  public void testValidateIncorrectAspect() {
    final Domains domains =
        new Domains()
            .setDomains(new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:domain:123"))));

    LineageDatasetUrnResolver test =
        new LineageDatasetUrnResolver().setConfig(mock(AspectPluginConfig.class));

    assertEquals(
        test.writeMutation(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(jobUrn)
                        .entitySpec(entityRegistry.getEntitySpec(jobUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(jobUrn.getEntityType())
                                .getAspectSpec(DOMAINS_ASPECT_NAME))
                        .recordTemplate(domains)
                        .build()),
                mockRetrieverContext)
            .filter(Pair::getSecond)
            .count(),
        0);
  }

  @Test
  public void testDatasetUrnResolution() {
    final DataJobInputOutput dataJobOtherPlatform =
        new DataJobInputOutput()
            .setInputDatasetEdges(
                new EdgeArray(new Edge().setDestinationUrn(upstreamUrnOtherPlatform)))
            .setOutputDatasetEdges(
                new EdgeArray(new Edge().setDestinationUrn(downstreamUrnOtherPlatform)));

    final DataJobInputOutput dataJobNeedingResolution =
        new DataJobInputOutput()
            .setInputDatasetEdges(
                new EdgeArray(
                    new Edge().setDestinationUrn(upstreamUrnOtherPlatform),
                    new Edge().setDestinationUrn(upstreamIcebergUrn),
                    new Edge().setDestinationUrn(upstreamNonExistentUrn),
                    new Edge().setDestinationUrn(upstreamExistingUrnNoResolution)))
            .setOutputDatasetEdges(
                new EdgeArray(
                    new Edge().setDestinationUrn(downstreamUrnOtherPlatform),
                    new Edge().setDestinationUrn(downstreamIcebergUrn)))
            .setFineGrainedLineages(
                new FineGrainedLineageArray(
                    new FineGrainedLineage()
                        .setUpstreams(
                            new UrnArray(
                                upstreamIcebergSchemaFieldUrn,
                                upstreamExistingSchemaFieldUrnNoResolution))
                        .setDownstreams(new UrnArray(downstreamIcebergSchemaFieldUrn))
                        .setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET)
                        .setDownstreamType(FineGrainedLineageDownstreamType.FIELD_SET)));

    when(mockAspectRetriever.entityExists(
            eq(
                Set.of(
                    upstreamIcebergUrn,
                    upstreamNonExistentUrn,
                    upstreamExistingUrnNoResolution,
                    downstreamIcebergUrn))))
        .thenReturn(
            Map.of(
                upstreamIcebergUrn,
                false,
                upstreamNonExistentUrn,
                false,
                upstreamExistingUrnNoResolution,
                true,
                downstreamIcebergUrn,
                false));

    when(mockAspectRetriever.getLatestAspectObjects(
            eq(Set.of(upstreamResourceUrn, downstreamResourceUrn, upstreamNonExistentResourceUrn)),
            eq(Set.of(PLATFORM_RESOURCE_INFO_ASPECT_NAME))))
        .thenReturn(
            Map.of(
                upstreamResourceUrn,
                Map.of(PLATFORM_RESOURCE_INFO_ASPECT_NAME, new Aspect(upstreamResourceInfo.data())),
                downstreamResourceUrn,
                Map.of(
                    PLATFORM_RESOURCE_INFO_ASPECT_NAME,
                    new Aspect(downstreamResourceInfo.data()))));

    LineageDatasetUrnResolver test =
        new LineageDatasetUrnResolver()
            .setConfig(mock(AspectPluginConfig.class))
            .setPlatforms(List.of("iceberg"))
            .setScoreUrnExists(1f)
            .setScoreUrnUnresolved(0f)
            .setScoreUrnResolvedPlatformResource(0.9f);

    Urn job2Urn = UrnUtils.getUrn("urn:li:dataJob:(urn:li:dataFlow:(spark,TestFlow,prod),Task2)");

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                List.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(jobUrn)
                        .entitySpec(entityRegistry.getEntitySpec(jobUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(jobUrn.getEntityType())
                                .getAspectSpec(DATA_JOB_INPUT_OUTPUT_ASPECT_NAME))
                        .recordTemplate(dataJobOtherPlatform)
                        .build(),
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(job2Urn)
                        .entitySpec(entityRegistry.getEntitySpec(job2Urn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(job2Urn.getEntityType())
                                .getAspectSpec(DATA_JOB_INPUT_OUTPUT_ASPECT_NAME))
                        .recordTemplate(dataJobNeedingResolution)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.size(), 2);
    assertFalse(result.get(0).getSecond());
    assertEquals(
        result.get(0).getFirst().getAspect(DataJobInputOutput.class).getInputDatasetEdges(),
        new EdgeArray(new Edge().setDestinationUrn(upstreamUrnOtherPlatform)));
    assertEquals(
        result.get(0).getFirst().getAspect(DataJobInputOutput.class).getOutputDatasetEdges(),
        new EdgeArray(new Edge().setDestinationUrn(downstreamUrnOtherPlatform)));

    assertTrue(result.get(1).getSecond());
    assertEquals(
        result.get(1).getFirst().getAspect(DataJobInputOutput.class).getInputDatasetEdges(),
        new EdgeArray(
            new Edge().setDestinationUrn(upstreamUrnOtherPlatform),
            new Edge().setDestinationUrn(upstreamIcebergUrnResolved).setConfidenceScore(0.9f),
            new Edge().setDestinationUrn(upstreamNonExistentUrn).setConfidenceScore(0f),
            new Edge().setDestinationUrn(upstreamExistingUrnNoResolution).setConfidenceScore(1f)));
    assertEquals(
        result.get(1).getFirst().getAspect(DataJobInputOutput.class).getOutputDatasetEdges(),
        new EdgeArray(
            new Edge().setDestinationUrn(downstreamUrnOtherPlatform),
            new Edge().setDestinationUrn(downstreamIcebergUrnResolved).setConfidenceScore(0.9f)));
    assertEquals(
        result.get(1).getFirst().getAspect(DataJobInputOutput.class).getFineGrainedLineages(),
        new FineGrainedLineageArray(
            new FineGrainedLineage()
                .setUpstreams(
                    new UrnArray(
                        upstreamIcebergSchemaFieldUrnResolved,
                        upstreamExistingSchemaFieldUrnNoResolution))
                .setDownstreams(new UrnArray(downstreamIcebergSchemaFieldUrnResolved))
                .setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET)
                .setDownstreamType(FineGrainedLineageDownstreamType.FIELD_SET)));
  }
}
