package com.linkedin.datahub.graphql.resolvers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.LineageEdge;
import com.linkedin.datahub.graphql.generated.UpdateLineageInput;
import com.linkedin.datahub.graphql.resolvers.lineage.UpdateLineageResolver;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import org.joda.time.DateTimeUtils;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class UpdateLineageResolverTest {

  private static EntityService _mockService = Mockito.mock(EntityService.class);
  private static DataFetchingEnvironment _mockEnv = Mockito.mock(DataFetchingEnvironment.class);
  private static AuditStamp _auditStamp;
  private static final String ACTOR_URN = "urn:li:corpuser:test";
  private static final String DATASET_URN_1 = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test1,DEV)";
  private static final String DATASET_URN_2 = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test2,DEV)";
  private static final String DATASET_URN_3 = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test3,DEV)";
  private static final String DATASET_URN_4 = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test4,DEV)";
  private static final String CHART_URN_1 = "urn:li:dataset:(looker,baz1)";

  @BeforeMethod
  public void setupTest() {
    DateTimeUtils.setCurrentMillisFixed(123L);
    _auditStamp = new AuditStamp().setActor(UrnUtils.getUrn(ACTOR_URN)).setTime(123L);
    _mockService = Mockito.mock(EntityService.class);
    _mockEnv = Mockito.mock(DataFetchingEnvironment.class);
  }

  // Adds upstream for dataset1 to dataset2 and removes edge to dataset3
  // Adds upstream for dataset3 to dataset4
  @Test
  public void testUpdateDatasetLineage() throws Exception {
    QueryContext mockContext = getMockAllowContext();

    UpdateLineageResolver resolver = new UpdateLineageResolver(_mockService);
    LineageEdge edgeToAdd1 = createLineageEdge(DATASET_URN_1, DATASET_URN_2);
    LineageEdge edgeToAdd2 = createLineageEdge(DATASET_URN_3, DATASET_URN_4);
    List<LineageEdge> edgesToAdd = new ArrayList<>(Arrays.asList(edgeToAdd1, edgeToAdd2));
    LineageEdge edgeToRemove = createLineageEdge(DATASET_URN_1, DATASET_URN_3);
    List<LineageEdge> edgesToRemove = new ArrayList<>(Collections.singletonList(edgeToRemove));

    UpdateLineageInput input = new UpdateLineageInput(edgesToAdd, edgesToRemove);
    Mockito.when(_mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(_mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_1))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_2))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_3))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_4))).thenReturn(true);

    UpstreamLineage upstreamLineage = createUpstreamLineage(new ArrayList<>(Arrays.asList(DATASET_URN_3, DATASET_URN_4)));
    Mockito.when(_mockService.getAspect(
            Mockito.eq(UrnUtils.getUrn(DATASET_URN_1)),
            Mockito.eq(Constants.UPSTREAM_LINEAGE_ASPECT_NAME),
            Mockito.eq(0L)))
        .thenReturn(upstreamLineage);

    Mockito.when(_mockService.getAspect(
            Mockito.eq(UrnUtils.getUrn(DATASET_URN_3)),
            Mockito.eq(Constants.UPSTREAM_LINEAGE_ASPECT_NAME),
            Mockito.eq(0L)))
        .thenReturn(null);

    assertTrue(resolver.get(_mockEnv).get());

    // upstreamLineage without dataset3, keep dataset4, add dataset2
    final UpstreamLineage updatedDataset1UpstreamLineage = createUpstreamLineage(new ArrayList<>(Arrays.asList(DATASET_URN_4, DATASET_URN_2)));
    final MetadataChangeProposal proposal1 = new MetadataChangeProposal();
    proposal1.setEntityUrn(UrnUtils.getUrn(DATASET_URN_1));
    proposal1.setEntityType(Constants.DATASET_ENTITY_NAME);
    proposal1.setAspectName(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
    proposal1.setAspect(GenericRecordUtils.serializeAspect(updatedDataset1UpstreamLineage));
    proposal1.setChangeType(ChangeType.UPSERT);
    Mockito.verify(_mockService, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal1),
        Mockito.any(AuditStamp.class),
        Mockito.eq(false)
    );

    // upstreamLineage adding dataset 4
    final UpstreamLineage updatedDataset3UpstreamLineage = createUpstreamLineage(new ArrayList<>(Collections.singletonList(DATASET_URN_4)));
    final MetadataChangeProposal proposal2 = new MetadataChangeProposal();
    proposal2.setEntityUrn(UrnUtils.getUrn(DATASET_URN_3));
    proposal2.setEntityType(Constants.DATASET_ENTITY_NAME);
    proposal2.setAspectName(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
    proposal2.setAspect(GenericRecordUtils.serializeAspect(updatedDataset3UpstreamLineage));
    proposal2.setChangeType(ChangeType.UPSERT);
    Mockito.verify(_mockService, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal2),
        Mockito.any(AuditStamp.class),
        Mockito.eq(false)
    );
  }

  @Test
  public void testFailUpdateWithMissingDownstream() throws Exception {
    QueryContext mockContext = getMockAllowContext();

    UpdateLineageResolver resolver = new UpdateLineageResolver(_mockService);
    LineageEdge edgeToAdd1 = createLineageEdge(DATASET_URN_1, DATASET_URN_2);
    List<LineageEdge> edgesToAdd = new ArrayList<>(Collections.singletonList(edgeToAdd1));
    List<LineageEdge> edgesToRemove = new ArrayList<>();
    UpdateLineageInput input = new UpdateLineageInput(edgesToAdd, edgesToRemove);
    Mockito.when(_mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(_mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_1))).thenReturn(false);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_2))).thenReturn(true);

    assertThrows(CompletionException.class, () -> resolver.get(_mockEnv).join());
  }

  @Test
  public void testFailUpdateWithMissingUpstream() throws Exception {
    QueryContext mockContext = getMockAllowContext();

    UpdateLineageResolver resolver = new UpdateLineageResolver(_mockService);
    LineageEdge edgeToAdd1 = createLineageEdge(DATASET_URN_1, DATASET_URN_2);
    List<LineageEdge> edgesToAdd = new ArrayList<>(Collections.singletonList(edgeToAdd1));
    List<LineageEdge> edgesToRemove = new ArrayList<>();
    UpdateLineageInput input = new UpdateLineageInput(edgesToAdd, edgesToRemove);
    Mockito.when(_mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(_mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_1))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_2))).thenReturn(false);

    assertThrows(CompletionException.class, () -> resolver.get(_mockEnv).join());
  }

  @Test
  public void testFailUpdateDatasetWithInvalidEdge() throws Exception {
    QueryContext mockContext = getMockAllowContext();

    UpdateLineageResolver resolver = new UpdateLineageResolver(_mockService);
    LineageEdge edgeToAdd1 = createLineageEdge(DATASET_URN_1, CHART_URN_1);
    List<LineageEdge> edgesToAdd = new ArrayList<>(Collections.singletonList(edgeToAdd1));
    List<LineageEdge> edgesToRemove = new ArrayList<>();
    UpdateLineageInput input = new UpdateLineageInput(edgesToAdd, edgesToRemove);
    Mockito.when(_mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(_mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_1))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(CHART_URN_1))).thenReturn(true);

    assertThrows(CompletionException.class, () -> resolver.get(_mockEnv).join());
  }

  // TODO: Add tests for permissions once we add permissions for updating lineage

  private LineageEdge createLineageEdge(String downstreamUrn, String upstreamUrn) {
    LineageEdge lineageEdge = new LineageEdge();
    lineageEdge.setDownstreamUrn(downstreamUrn);
    lineageEdge.setUpstreamUrn(upstreamUrn);
    return lineageEdge;
  }

  private UpstreamLineage createUpstreamLineage(List<String> upstreamUrns) throws Exception {
    UpstreamLineage upstreamLineage = new UpstreamLineage();
    UpstreamArray upstreams = new UpstreamArray();
    for (String upstreamUrn : upstreamUrns) {
      Upstream upstream = new Upstream();
      upstream.setDataset(DatasetUrn.createFromString(upstreamUrn));
      upstream.setAuditStamp(_auditStamp);
      upstream.setCreatedAuditStamp(_auditStamp);
      upstream.setType(DatasetLineageType.TRANSFORMED);
      upstreams.add(upstream);
    }
    upstreamLineage.setUpstreams(upstreams);
    return upstreamLineage;
  }
}
