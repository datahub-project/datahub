package com.linkedin.datahub.upgrade.test;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeReport;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.test.TestResults;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.upgrade.propagate.PropagateTerms.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;


public class EvaluateTestsStepTest {
  private static final Urn DATASET_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");
  private static final Urn CHART_URN1 = UrnUtils.getUrn("urn:li:chart:(looker,SampleChartOne)");
  private static final Urn CHART_URN2 = UrnUtils.getUrn("urn:li:chart:(looker,SampleChartTwo)");
  private static final String SCROLL_ID = "test123";

  @Test
  public void testExecutable() {
    final EntitySearchService entitySearchService = mock(EntitySearchService.class);
    final TestEngine testEngine = mock(TestEngine.class);
    configureTestEngineMock(testEngine);
    configureEntitySearchServiceMock(entitySearchService);
    EntityClient entityClient = Mockito.mock(EntityClient.class);
    Authentication authentication = Mockito.mock(Authentication.class);

    EvaluateTestsStep testStep = new EvaluateTestsStep(entityClient, entitySearchService, testEngine, authentication);
    Function<UpgradeContext, UpgradeStepResult> fun = testStep.executable();
    final UpgradeContext upgradeContext = mock(UpgradeContext.class);
    UpgradeReport report = new DefaultUpgradeReport();
    configureUpgradeCtxMock(upgradeContext, report);
    fun.apply(upgradeContext);
    List<String> expectedLines = List.of("Starting to evaluate tests...",
        "Evaluating tests for entities [dataset, chart]",
        "Fetching batch 1 of dataset entities",
        "Processing batch 1 of dataset entities",
        "Finished submitting test evaluation for dataset entities to worker pool.",
        "Fetching batch 1 of chart entities",
        "Pushed 1 test results for batch 1 of dataset entities",
        "Processing batch 1 of chart entities",
        "Fetching batch 2 of chart entities",
        "Pushed 1 test results for batch 1 of chart entities",
        "Processing batch 2 of chart entities",
        "Finished submitting test evaluation for chart entities to worker pool.",
        "Pushed 1 test results for batch 2 of chart entities",
        "Finished evaluating tests for all entities");
    assertEquals(expectedLines.size(), report.lines().size());
    // Start with "starting to evaluate tests" line
    assertEquals(expectedLines.get(0), report.lines().get(0));
    assertTrue(expectedLines.get(1).startsWith("Evaluating tests for entities ["));
    assertTrue(expectedLines.get(1).equals("Evaluating tests for entities [dataset, chart]")
            || expectedLines.get(1).equals("Evaluating tests for entities [chart, dataset]")
        );
    // end with "finished evaluating tests" line
    assertEquals(expectedLines.get(expectedLines.size() - 1),
        report.lines().get(expectedLines.size() - 1));

    // Don't check exact order of others since executor may execute them in a different order
    // make sure only fetched one batch of datasets
    assertTrue(report.lines().contains("Fetching batch 1 of dataset entities"));
    assertFalse(report.lines().contains("Fetching batch 2 of dataset entities"));
    // make sure fetched two batches of charts
    assertTrue(report.lines().contains("Fetching batch 1 of chart entities"));
    assertTrue(report.lines().contains("Fetching batch 2 of chart entities"));
  }

  private static void configureTestEngineMock(final TestEngine mockTestEngine) {
    Mockito.when(mockTestEngine.getEntityTypesToEvaluate())
        .thenReturn(Set.of(Constants.DATASET_ENTITY_NAME,
            Constants.CHART_ENTITY_NAME));

    for (Urn urn : List.of(DATASET_URN, CHART_URN1, CHART_URN2)) {
      Mockito.when(mockTestEngine.batchEvaluateTestsForEntities(List.of(urn), TestEngine.EvaluationMode.DEFAULT))
          .thenReturn(Map.of(urn, new TestResults()));
    }
  }

  private static void configureUpgradeCtxMock(final UpgradeContext mockUpgradeContext, UpgradeReport report) {
    Mockito.when(mockUpgradeContext.report()).thenReturn(report);
    Mockito.when(mockUpgradeContext.parsedArgs()).thenReturn(Map.of("BATCH_SIZE", Optional.of("1")));
  }

  private static void configureEntitySearchServiceMock(final EntitySearchService mockSearchService) {
    SearchEntity datasetSearchEntry = new SearchEntity();
    datasetSearchEntry.setEntity(DATASET_URN);
    SearchEntityArray datasetSearchEntryArray = new SearchEntityArray();
    datasetSearchEntryArray.add(datasetSearchEntry);
    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setEntities(datasetSearchEntryArray);
    // null scroll ID

    Mockito.when(mockSearchService.scroll(
        Mockito.eq(Collections.singletonList(Constants.DATASET_ENTITY_NAME)),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.eq(1),
        Mockito.eq(null),
        Mockito.eq(ELASTIC_TIMEOUT)
    )).thenReturn(scrollResult);

    SearchEntity chartOneSearchEntry = new SearchEntity();
    chartOneSearchEntry.setEntity(CHART_URN1);
    SearchEntityArray chartOneSearchEntityArray = new SearchEntityArray();
    chartOneSearchEntityArray.add(chartOneSearchEntry);
    ScrollResult chartOneScrollResult = new ScrollResult();
    chartOneScrollResult.setEntities(chartOneSearchEntityArray);
    chartOneScrollResult.setScrollId(SCROLL_ID);

    Mockito.when(mockSearchService.scroll(
        Mockito.eq(Collections.singletonList(Constants.CHART_ENTITY_NAME)),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.eq(1),
        Mockito.eq(null),
        Mockito.eq(ELASTIC_TIMEOUT)
    )).thenReturn(chartOneScrollResult);

    SearchEntity chartTwoSearchEntry = new SearchEntity();
    chartTwoSearchEntry.setEntity(CHART_URN1);
    SearchEntityArray chartTwoSearchEntityArray = new SearchEntityArray();
    chartTwoSearchEntityArray.add(chartTwoSearchEntry);
    ScrollResult chartTwoScrollResult = new ScrollResult();
    chartTwoScrollResult.setEntities(chartTwoSearchEntityArray);
    // Null scroll ID

    Mockito.when(mockSearchService.scroll(
        Mockito.eq(Collections.singletonList(Constants.CHART_ENTITY_NAME)),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.eq(1),
        Mockito.eq(SCROLL_ID),
        Mockito.eq(ELASTIC_TIMEOUT)
    )).thenReturn(chartTwoScrollResult);
  }

}