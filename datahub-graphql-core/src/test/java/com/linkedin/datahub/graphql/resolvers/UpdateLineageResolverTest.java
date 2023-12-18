package com.linkedin.datahub.graphql.resolvers;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.LineageEdge;
import com.linkedin.datahub.graphql.generated.UpdateLineageInput;
import com.linkedin.datahub.graphql.resolvers.lineage.UpdateLineageResolver;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.LineageService;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.joda.time.DateTimeUtils;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateLineageResolverTest {

  private static EntityService _mockService = Mockito.mock(EntityService.class);
  private static LineageService _lineageService;
  private static DataFetchingEnvironment _mockEnv;
  private static final String DATASET_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,test1,DEV)";
  private static final String DATASET_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,test2,DEV)";
  private static final String DATASET_URN_3 =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,test3,DEV)";
  private static final String DATASET_URN_4 =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,test4,DEV)";
  private static final String CHART_URN = "urn:li:chart:(looker,baz)";
  private static final String DASHBOARD_URN = "urn:li:dashboard:(airflow,id)";
  private static final String DATAJOB_URN_1 =
      "urn:li:dataJob:(urn:li:dataFlow:(airflow,test,prod),test1)";
  private static final String DATAJOB_URN_2 =
      "urn:li:dataJob:(urn:li:dataFlow:(airflow,test,prod),test2)";

  @BeforeMethod
  public void setupTest() {
    DateTimeUtils.setCurrentMillisFixed(123L);
    _mockService = Mockito.mock(EntityService.class);
    _lineageService = Mockito.mock(LineageService.class);
    _mockEnv = Mockito.mock(DataFetchingEnvironment.class);
  }

  // Adds upstream for dataset1 to dataset2 and removes edge to dataset3
  @Test
  public void testUpdateDatasetLineage() throws Exception {
    List<LineageEdge> edgesToAdd =
        Arrays.asList(
            createLineageEdge(DATASET_URN_1, DATASET_URN_2),
            createLineageEdge(DATASET_URN_3, DATASET_URN_4));
    List<LineageEdge> edgesToRemove =
        Arrays.asList(createLineageEdge(DATASET_URN_1, DATASET_URN_3));
    mockInputAndContext(edgesToAdd, edgesToRemove);
    UpdateLineageResolver resolver = new UpdateLineageResolver(_mockService, _lineageService);

    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_1))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_2))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_3))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_4))).thenReturn(true);

    assertTrue(resolver.get(_mockEnv).get());
  }

  @Test
  public void testFailUpdateWithMissingDownstream() throws Exception {
    List<LineageEdge> edgesToAdd =
        Collections.singletonList(createLineageEdge(DATASET_URN_1, DATASET_URN_2));
    mockInputAndContext(edgesToAdd, new ArrayList<>());
    UpdateLineageResolver resolver = new UpdateLineageResolver(_mockService, _lineageService);

    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_1))).thenReturn(false);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_2))).thenReturn(false);

    assertThrows(CompletionException.class, () -> resolver.get(_mockEnv).join());
  }

  // Adds upstream for chart1 to dataset2 and removes edge to dataset1
  @Test
  public void testUpdateChartLineage() throws Exception {
    List<LineageEdge> edgesToAdd = Arrays.asList(createLineageEdge(CHART_URN, DATASET_URN_2));
    List<LineageEdge> edgesToRemove = Arrays.asList(createLineageEdge(CHART_URN, DATASET_URN_1));
    mockInputAndContext(edgesToAdd, edgesToRemove);
    UpdateLineageResolver resolver = new UpdateLineageResolver(_mockService, _lineageService);

    Mockito.when(_mockService.exists(Urn.createFromString(CHART_URN))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_2))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_1))).thenReturn(true);

    assertTrue(resolver.get(_mockEnv).get());
  }

  // Adds upstream for dashboard to dataset2 and chart1 and removes edge to dataset1
  @Test
  public void testUpdateDashboardLineage() throws Exception {
    List<LineageEdge> edgesToAdd =
        Arrays.asList(
            createLineageEdge(DASHBOARD_URN, DATASET_URN_2),
            createLineageEdge(DASHBOARD_URN, CHART_URN));
    List<LineageEdge> edgesToRemove =
        Arrays.asList(createLineageEdge(DASHBOARD_URN, DATASET_URN_1));
    mockInputAndContext(edgesToAdd, edgesToRemove);
    UpdateLineageResolver resolver = new UpdateLineageResolver(_mockService, _lineageService);

    Mockito.when(_mockService.exists(Urn.createFromString(DASHBOARD_URN))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_2))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_1))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(CHART_URN))).thenReturn(true);

    assertTrue(resolver.get(_mockEnv).get());
  }

  // Adds upstream datajob and dataset and one downstream dataset
  @Test
  public void testUpdateDataJobLineage() throws Exception {
    List<LineageEdge> edgesToAdd =
        Arrays.asList(
            createLineageEdge(DATAJOB_URN_1, DATASET_URN_2),
            createLineageEdge(DATAJOB_URN_1, DATAJOB_URN_2),
            createLineageEdge(DATASET_URN_3, DATAJOB_URN_1));
    List<LineageEdge> edgesToRemove =
        Arrays.asList(createLineageEdge(DATAJOB_URN_1, DATASET_URN_1));
    mockInputAndContext(edgesToAdd, edgesToRemove);
    UpdateLineageResolver resolver = new UpdateLineageResolver(_mockService, _lineageService);

    Mockito.when(_mockService.exists(Urn.createFromString(DATAJOB_URN_1))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_2))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATAJOB_URN_2))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_1))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_3))).thenReturn(true);

    assertTrue(resolver.get(_mockEnv).get());
  }

  @Test
  public void testFailUpdateLineageNoPermissions() throws Exception {
    List<LineageEdge> edgesToAdd =
        Arrays.asList(
            createLineageEdge(DATASET_URN_1, DATASET_URN_2),
            createLineageEdge(DATASET_URN_3, DATASET_URN_4));
    List<LineageEdge> edgesToRemove =
        Arrays.asList(createLineageEdge(DATASET_URN_1, DATASET_URN_3));

    QueryContext mockContext = getMockDenyContext();
    UpdateLineageInput input = new UpdateLineageInput(edgesToAdd, edgesToRemove);
    Mockito.when(_mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(_mockEnv.getContext()).thenReturn(mockContext);

    UpdateLineageResolver resolver = new UpdateLineageResolver(_mockService, _lineageService);

    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_1))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_2))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_3))).thenReturn(true);
    Mockito.when(_mockService.exists(Urn.createFromString(DATASET_URN_4))).thenReturn(true);

    assertThrows(AuthorizationException.class, () -> resolver.get(_mockEnv).join());
  }

  private void mockInputAndContext(List<LineageEdge> edgesToAdd, List<LineageEdge> edgesToRemove) {
    QueryContext mockContext = getMockAllowContext();
    UpdateLineageInput input = new UpdateLineageInput(edgesToAdd, edgesToRemove);
    Mockito.when(_mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(_mockEnv.getContext()).thenReturn(mockContext);
  }

  private LineageEdge createLineageEdge(String downstreamUrn, String upstreamUrn) {
    LineageEdge lineageEdge = new LineageEdge();
    lineageEdge.setDownstreamUrn(downstreamUrn);
    lineageEdge.setUpstreamUrn(upstreamUrn);
    return lineageEdge;
  }
}
