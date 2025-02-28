package com.linkedin.metadata.boot.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.common.AssertionSummaryDetails;
import com.linkedin.common.AssertionSummaryDetailsArray;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.ScrollConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class MigrateAssertionsSummaryStepTest {

  private static final Urn ASSERTION_URN =
      UrnUtils.getUrn("urn:li:assertion:126d8dc8939e0cf9bf0fd03264ad1a06");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");
  private static final String SCROLL_ID = "test123";

  @Test
  public void testExecuteAssertionsSummaryStepWithFailingAssertion() throws Exception {
    final EntityService entityService = mock(EntityService.class);
    final EntitySearchService entitySearchService = mock(EntitySearchService.class);
    final AssertionService assertionService = mock(AssertionService.class);
    final TimeseriesAspectService timeseriesAspectService = mock(TimeseriesAspectService.class);
    final ConfigurationProvider configurationProvider = createConfigurationProvider();
    configureEntitySearchServiceMock(entitySearchService);
    configureAssertionServiceMock(assertionService);
    configureTimeSeriesAspectServiceMock(timeseriesAspectService, AssertionResultType.FAILURE);

    final MigrateAssertionsSummaryStep step =
        new MigrateAssertionsSummaryStep(
            entityService,
            entitySearchService,
            assertionService,
            timeseriesAspectService,
            configurationProvider);

    step.upgrade(mock(OperationContext.class));

    AssertionsSummary expectedSummary = new AssertionsSummary();
    AssertionSummaryDetailsArray failingSummaryDetails = new AssertionSummaryDetailsArray();
    failingSummaryDetails.add(buildAssertionSummaryDetails(ASSERTION_URN));
    expectedSummary.setFailingAssertionDetails(failingSummaryDetails);
    expectedSummary.setPassingAssertions(new UrnArray());
    expectedSummary.setFailingAssertions(new UrnArray());

    Mockito.verify(assertionService, times(1))
        .updateAssertionsSummary(
            any(OperationContext.class), Mockito.eq(DATASET_URN), Mockito.eq(expectedSummary));
  }

  private ConfigurationProvider createConfigurationProvider() {
    ConfigurationProvider configurationProvider = new ConfigurationProvider();
    final ElasticSearchConfiguration elasticSearchConfiguration = new ElasticSearchConfiguration();
    final ScrollConfiguration scrollConfiguration = new ScrollConfiguration();
    scrollConfiguration.setTimeout("5m");
    elasticSearchConfiguration.setScroll(scrollConfiguration);
    configurationProvider.setElasticSearch(elasticSearchConfiguration);

    return configurationProvider;
  }

  @Test
  public void testExecuteAssertionsSummaryStepWithPassingAssertion() throws Exception {
    final EntityService entityService = mock(EntityService.class);
    final EntitySearchService entitySearchService = mock(EntitySearchService.class);
    final AssertionService assertionService = mock(AssertionService.class);
    final TimeseriesAspectService timeseriesAspectService = mock(TimeseriesAspectService.class);
    final ConfigurationProvider configurationProvider = createConfigurationProvider();
    configureEntitySearchServiceMock(entitySearchService);
    configureAssertionServiceMock(assertionService);
    configureTimeSeriesAspectServiceMock(timeseriesAspectService, AssertionResultType.SUCCESS);

    final MigrateAssertionsSummaryStep step =
        new MigrateAssertionsSummaryStep(
            entityService,
            entitySearchService,
            assertionService,
            timeseriesAspectService,
            configurationProvider);

    step.upgrade(mock(OperationContext.class));

    AssertionsSummary expectedSummary = new AssertionsSummary();
    AssertionSummaryDetailsArray passingAssertionSummary = new AssertionSummaryDetailsArray();
    passingAssertionSummary.add(buildAssertionSummaryDetails(ASSERTION_URN));
    expectedSummary.setPassingAssertionDetails(passingAssertionSummary);
    expectedSummary.setPassingAssertions(new UrnArray());
    expectedSummary.setFailingAssertions(new UrnArray());

    Mockito.verify(assertionService, times(1))
        .updateAssertionsSummary(
            any(OperationContext.class), Mockito.eq(DATASET_URN), Mockito.eq(expectedSummary));
  }

  private static void configureEntitySearchServiceMock(
      final EntitySearchService mockSearchService) {
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(ASSERTION_URN);
    SearchEntityArray searchEntityArray = new SearchEntityArray();
    searchEntityArray.add(searchEntity);
    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setEntities(searchEntityArray);
    scrollResult.setScrollId(SCROLL_ID);

    Mockito.when(
            mockSearchService.scroll(
                Mockito.any(),
                Mockito.eq(Collections.singletonList(Constants.ASSERTION_ENTITY_NAME)),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1000),
                Mockito.eq(null),
                Mockito.eq("5m"),
                Mockito.eq(null)))
        .thenReturn(scrollResult);

    ScrollResult newScrollResult = new ScrollResult();
    newScrollResult.setEntities(new SearchEntityArray());

    Mockito.when(
            mockSearchService.scroll(
                Mockito.any(),
                Mockito.eq(Collections.singletonList(Constants.ASSERTION_ENTITY_NAME)),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1000),
                Mockito.eq(SCROLL_ID),
                Mockito.eq("5m"),
                Mockito.eq(null)))
        .thenReturn(newScrollResult);
  }

  private static void configureAssertionServiceMock(final AssertionService mockAssertionService) {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.DATASET);
    assertionInfo.setDatasetAssertion(
        new DatasetAssertionInfo()
            .setDataset(DATASET_URN)
            .setOperator(AssertionStdOperator.CONTAIN)
            .setScope(DatasetAssertionScope.DATASET_COLUMN)
            .setAggregation(AssertionStdAggregation.MAX));
    assertionInfo.setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL));

    Mockito.when(
            mockAssertionService.getAssertionInfo(
                any(OperationContext.class), Mockito.eq(ASSERTION_URN)))
        .thenReturn(assertionInfo);

    Mockito.when(
            mockAssertionService.getAssertionsSummary(
                any(OperationContext.class), Mockito.eq(DATASET_URN)))
        .thenReturn(
            new AssertionsSummary()
                .setPassingAssertions(new UrnArray(ImmutableList.of(ASSERTION_URN)))
                .setFailingAssertions(new UrnArray(ImmutableList.of(ASSERTION_URN))));
  }

  private static void configureTimeSeriesAspectServiceMock(
      final TimeseriesAspectService timeseriesAspectService, final AssertionResultType resultType) {
    List<EnvelopedAspect> envelopedAspects = new ArrayList<>();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    AssertionRunEvent assertionRunEvent = new AssertionRunEvent();
    AssertionResult assertionResult = new AssertionResult();
    assertionResult.setType(resultType);
    assertionRunEvent.setResult(assertionResult);
    assertionRunEvent.setTimestampMillis(1L);
    envelopedAspect.setAspect(GenericRecordUtils.serializeAspect(assertionRunEvent));
    envelopedAspects.add(envelopedAspect);

    Mockito.when(
            timeseriesAspectService.getAspectValues(
                any(OperationContext.class),
                Mockito.eq(ASSERTION_URN),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1),
                Mockito.eq(null),
                Mockito.eq(null)))
        .thenReturn(envelopedAspects);
  }

  @Nonnull
  private AssertionSummaryDetails buildAssertionSummaryDetails(@Nonnull final Urn urn) {
    AssertionSummaryDetails assertionSummaryDetails = new AssertionSummaryDetails();
    assertionSummaryDetails.setUrn(urn);
    assertionSummaryDetails.setType(AssertionType.DATASET.toString());
    assertionSummaryDetails.setLastResultAt(1L);
    assertionSummaryDetails.setSource(AssertionSourceType.EXTERNAL.toString());
    return assertionSummaryDetails;
  }
}
