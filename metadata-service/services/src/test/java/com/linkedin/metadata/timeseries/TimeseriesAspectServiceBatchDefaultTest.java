package com.linkedin.metadata.timeseries;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.GenericTable;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for the {@code batchGetAggregatedStats} default methods on {@link
 * TimeseriesAspectService}. These exercise the interface's fallback fan-out implementation (one
 * {@link TimeseriesAspectService#getAggregatedStats} call per URN) and the URN-criterion injection
 * logic, independent of any Elasticsearch-backed implementation.
 */
public class TimeseriesAspectServiceBatchDefaultTest {

  private static final String ENTITY_NAME = "dataset";
  private static final String ASPECT_NAME = "datasetProfile";
  private static final Urn URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,one,PROD)");
  private static final Urn URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,two,PROD)");
  private static final AggregationSpec[] AGG_SPECS = new AggregationSpec[] {new AggregationSpec()};

  private TimeseriesAspectService service;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    // CALLS_REAL_METHODS runs the interface default methods while leaving the abstract
    // getAggregatedStats a stubbable seam.
    service = mock(TimeseriesAspectService.class, Mockito.CALLS_REAL_METHODS);
    opContext = mock(OperationContext.class);
    when(service.getAggregatedStats(
            any(), any(), any(), any(AggregationSpec[].class), any(), any()))
        .thenReturn(new GenericTable());
  }

  @Test
  public void testFansOutOncePerUrn() {
    Map<Urn, GenericTable> result =
        service.batchGetAggregatedStats(
            opContext, ENTITY_NAME, ASPECT_NAME, AGG_SPECS, List.of(URN_1, URN_2), null, null);

    Assert.assertEquals(result.keySet(), java.util.Set.of(URN_1, URN_2));
    verify(service, times(2))
        .getAggregatedStats(
            any(), eq(ENTITY_NAME), eq(ASPECT_NAME), eq(AGG_SPECS), any(), Mockito.isNull());
  }

  @Test
  public void testEmptyUrnListReturnsEmptyMap() {
    Map<Urn, GenericTable> result =
        service.batchGetAggregatedStats(
            opContext, ENTITY_NAME, ASPECT_NAME, AGG_SPECS, List.of(), null, null);

    Assert.assertTrue(result.isEmpty());
    verify(service, times(0)).getAggregatedStats(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testDefaultUrnFieldPathIsUrn() {
    service.batchGetAggregatedStats(
        opContext, ENTITY_NAME, ASPECT_NAME, AGG_SPECS, List.of(URN_1), null, null);

    Criterion c = singleCriterion(captureFilter());
    Assert.assertEquals(c.getField(), "urn");
    Assert.assertEquals(c.getCondition(), Condition.EQUAL);
    Assert.assertEquals(c.getValues(), List.of(URN_1.toString()));
  }

  @Test
  public void testCustomUrnFieldPath() {
    service.batchGetAggregatedStats(
        opContext, ENTITY_NAME, ASPECT_NAME, AGG_SPECS, List.of(URN_1), null, null, "asserteeUrn");

    Criterion c = singleCriterion(captureFilter());
    Assert.assertEquals(c.getField(), "asserteeUrn");
    Assert.assertEquals(c.getValues(), List.of(URN_1.toString()));
  }

  @Test
  public void testNullSharedFilterProducesSingleConjunction() {
    service.batchGetAggregatedStats(
        opContext, ENTITY_NAME, ASPECT_NAME, AGG_SPECS, List.of(URN_1), null, null);

    Filter filter = captureFilter();
    Assert.assertEquals(filter.getOr().size(), 1);
    Assert.assertEquals(filter.getOr().get(0).getAnd().size(), 1);
  }

  @Test
  public void testUrnCriterionAppendedToEachExistingConjunction() {
    // A shared filter with two OR clauses; the urn criterion must be ANDed into both while the
    // original criteria are preserved.
    Criterion shared1 = CriterionUtils.buildCriterion("platform", Condition.EQUAL, "kafka");
    Criterion shared2 = CriterionUtils.buildCriterion("origin", Condition.EQUAL, "PROD");
    Filter sharedFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(shared1)),
                    new ConjunctiveCriterion().setAnd(new CriterionArray(shared2))));

    service.batchGetAggregatedStats(
        opContext, ENTITY_NAME, ASPECT_NAME, AGG_SPECS, List.of(URN_1), sharedFilter, null);

    Filter filter = captureFilter();
    Assert.assertEquals(filter.getOr().size(), 2);
    for (ConjunctiveCriterion cc : filter.getOr()) {
      Assert.assertEquals(cc.getAnd().size(), 2);
      Assert.assertTrue(
          cc.getAnd().stream()
              .anyMatch(
                  c -> c.getField().equals("urn") && c.getValues().contains(URN_1.toString())));
    }
    // Original shared filter must not be mutated.
    Assert.assertEquals(sharedFilter.getOr().get(0).getAnd().size(), 1);
    Assert.assertEquals(sharedFilter.getOr().get(1).getAnd().size(), 1);
  }

  @Test
  public void testEachUrnGetsItsOwnCriterion() {
    service.batchGetAggregatedStats(
        opContext, ENTITY_NAME, ASPECT_NAME, AGG_SPECS, List.of(URN_1, URN_2), null, null);

    ArgumentCaptor<Filter> captor = ArgumentCaptor.forClass(Filter.class);
    verify(service, times(2))
        .getAggregatedStats(
            any(), any(), any(), any(AggregationSpec[].class), captor.capture(), any());

    List<String> capturedUrns =
        captor.getAllValues().stream().map(f -> singleCriterion(f).getValues().get(0)).toList();
    Assert.assertTrue(capturedUrns.contains(URN_1.toString()));
    Assert.assertTrue(capturedUrns.contains(URN_2.toString()));
  }

  private Filter captureFilter() {
    ArgumentCaptor<Filter> captor = ArgumentCaptor.forClass(Filter.class);
    verify(service)
        .getAggregatedStats(
            any(), any(), any(), any(AggregationSpec[].class), captor.capture(), any());
    return captor.getValue();
  }

  private static Criterion singleCriterion(Filter filter) {
    ConjunctiveCriterion cc = filter.getOr().get(0);
    return cc.getAnd().get(cc.getAnd().size() - 1);
  }
}
