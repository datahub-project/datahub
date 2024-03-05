package com.linkedin.datahub.upgrade.propagate;

import static com.linkedin.datahub.upgrade.propagate.PropagateTerms.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeReport;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class PropagateTermsStepTest {
  private static final Urn DATASET_URN1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");
  private static final Urn DATASET_URN2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset2,PROD)");
  private static final String SCROLL_ID = "test123";

  @Test
  public void testExecutable() {
    final EntityService entityService = mock(EntityService.class);
    final EntitySearchService entitySearchService = mock(EntitySearchService.class);
    configureEntitySearchServiceMock(entitySearchService);

    PropagateTermsStep propagateTermsStep =
        new PropagateTermsStep(mock(OperationContext.class), entityService, entitySearchService);

    Function<UpgradeContext, UpgradeStepResult> fun = propagateTermsStep.executable();
    final UpgradeContext upgradeContext = mock(UpgradeContext.class);
    UpgradeReport report = new DefaultUpgradeReport();
    configureUpgradeCtxMock(upgradeContext, report);
    fun.apply(upgradeContext);

    // Run ID will be different, but the rest of the lines should be equal
    List<String> expectedLines =
        List.of(
            "Starting term propagation (Run ID: term_propagation_1685663328210_KhOv)...",
            "Fetching source entities to propagate from",
            "Found 0 source entities",
            "Fetching schema for the source entities",
            "Fetching all other entities",
            "Fetching batch 1",
            "Processing batch 1",
            "Fetching batch 2",
            "Processing batch 2",
            "Batch 2 is the last. Finishing job.",
            "Finished term propagation (Run ID: term_propagation_1685663328210_KhOv). Ingested 0 aspects");
    assertEquals(expectedLines.size(), report.lines().size());
    assertTrue(report.lines().get(0).startsWith("Starting term propagation (Run ID:"));
    for (int i = 1; i < expectedLines.size() - 2; i++) {
      assertEquals(expectedLines.get(i), report.lines().get(i));
    }
    assertTrue(
        report
            .lines()
            .get(expectedLines.size() - 1)
            .startsWith("Finished term propagation (Run ID:"));
    assertTrue(report.lines().get(expectedLines.size() - 1).endsWith("). Ingested 0 aspects"));
  }

  private static void configureUpgradeCtxMock(
      final UpgradeContext mockUpgradeContext, UpgradeReport report) {
    Mockito.when(mockUpgradeContext.report()).thenReturn(report);
    Mockito.when(mockUpgradeContext.args()).thenReturn(List.of("SOURCE_FILTER=a-b"));
  }

  private static void configureEntitySearchServiceMock(
      final EntitySearchService mockSearchService) {
    SearchEntity datasetSearchEntry = new SearchEntity();
    datasetSearchEntry.setEntity(DATASET_URN1);
    SearchEntityArray datasetSearchArray = new SearchEntityArray();
    datasetSearchArray.add(datasetSearchEntry);
    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setEntities(datasetSearchArray);
    scrollResult.setScrollId(SCROLL_ID);

    Mockito.when(
            mockSearchService.scroll(
                Mockito.any(),
                Mockito.eq(Collections.singletonList(Constants.DATASET_ENTITY_NAME)),
                Mockito.any(),
                Mockito.eq(null),
                Mockito.eq(1000),
                Mockito.eq(null),
                Mockito.eq(ELASTIC_TIMEOUT),
                Mockito.eq(null)))
        .thenReturn(scrollResult);

    SearchEntity datasetSearchEntry2 = new SearchEntity();
    datasetSearchEntry2.setEntity(DATASET_URN2);
    SearchEntityArray datasetSearchArray2 = new SearchEntityArray();
    datasetSearchArray2.add(datasetSearchEntry2);
    ScrollResult scrollResult2 = new ScrollResult();
    scrollResult2.setEntities(datasetSearchArray2);
    // Null scroll ID

    Mockito.when(
            mockSearchService.scroll(
                Mockito.any(),
                Mockito.eq(Collections.singletonList(Constants.DATASET_ENTITY_NAME)),
                Mockito.any(),
                Mockito.eq(null),
                Mockito.eq(1000),
                Mockito.eq(SCROLL_ID),
                Mockito.eq(ELASTIC_TIMEOUT),
                Mockito.eq(null)))
        .thenReturn(scrollResult2);

    Mockito.when(
            mockSearchService.filter(
                Mockito.any(),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.any(),
                Mockito.eq(null),
                Mockito.eq(0),
                Mockito.eq(5000)))
        .thenReturn(new SearchResult().setNumEntities(0).setEntities(new SearchEntityArray()));
  }
}
