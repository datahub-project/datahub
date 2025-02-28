package com.linkedin.metadata.boot.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.FixedIntervalSchedule;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessCronSchedule;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.timeseries.CalendarInterval;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class MigrateFreshnessAssertionCronToSinceTheLastCheckTest {

  private static final Urn ASSERTION_URN =
      UrnUtils.getUrn("urn:li:assertion:126d8dc8939e0cf9bf0fd03264ad1a06");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");
  private static final Urn ASSERTION_URN_2 =
      UrnUtils.getUrn("urn:li:assertion:126d8dc8939e0cf9bf0fd03264ad1a05");
  private static final String SCROLL_ID = "test123";

  @Test
  public void testUpdateCronAssertionSuccess() throws Exception {
    final EntityService entityService = mock(EntityService.class);
    final SearchService searchService = mock(SearchService.class);
    final AssertionService assertionService = mock(AssertionService.class);

    configureEntitySearchServiceMock(searchService);
    configureAssertionServiceMock(assertionService, true);

    final MigrateFreshnessAssertionCronToSinceTheLastCheck step =
        new MigrateFreshnessAssertionCronToSinceTheLastCheck(
            entityService, searchService, assertionService);

    step.upgrade(mock(OperationContext.class));

    Mockito.verify(entityService, Mockito.times(2))
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            Mockito.anyBoolean());
  }

  @Test
  public void testNoCronAssertionsToUpdate() throws Exception {
    final EntityService entityService = mock(EntityService.class);
    final SearchService searchService = mock(SearchService.class);
    final AssertionService assertionService = mock(AssertionService.class);

    configureEntitySearchServiceMock(searchService);
    configureAssertionServiceMock(assertionService, false);

    final MigrateFreshnessAssertionCronToSinceTheLastCheck step =
        new MigrateFreshnessAssertionCronToSinceTheLastCheck(
            entityService, searchService, assertionService);

    step.upgrade(mock(OperationContext.class));

    Mockito.verify(entityService, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            Mockito.anyBoolean());
  }

  private static void configureEntitySearchServiceMock(final SearchService searchService) {

    ScrollResult scrollResult =
        new ScrollResult()
            .setNumEntities(1)
            .setEntities(new SearchEntityArray(new SearchEntity().setEntity(ASSERTION_URN)));
    scrollResult.setScrollId(SCROLL_ID);

    Mockito.when(
            searchService.scrollAcrossEntities(
                any(),
                Mockito.eq(Collections.singletonList(Constants.ASSERTION_ENTITY_NAME)),
                Mockito.eq("*"),
                any(Filter.class),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq("5m"),
                Mockito.eq(5000),
                Mockito.eq(null)))
        .thenReturn(scrollResult);

    ScrollResult newScrollResult =
        new ScrollResult()
            .setNumEntities(1)
            .setEntities(new SearchEntityArray(new SearchEntity().setEntity(ASSERTION_URN_2)));

    Mockito.when(
            searchService.scrollAcrossEntities(
                any(),
                Mockito.eq(Collections.singletonList(Constants.ASSERTION_ENTITY_NAME)),
                Mockito.eq("*"),
                any(Filter.class),
                Mockito.eq(null),
                Mockito.eq(SCROLL_ID),
                Mockito.eq("5m"),
                Mockito.eq(5000),
                Mockito.eq(null)))
        .thenReturn(newScrollResult);
  }

  private void configureAssertionServiceMock(
      final AssertionService assertionService, final boolean isCron) {
    Mockito.when(
            assertionService.getAssertionInfo(
                any(OperationContext.class), Mockito.eq(ASSERTION_URN)))
        .thenReturn(buildAssertionInfo(isCron));
    Mockito.when(
            assertionService.getAssertionInfo(
                any(OperationContext.class), Mockito.eq(ASSERTION_URN_2)))
        .thenReturn(buildAssertionInfo(isCron));
  }

  private AssertionInfo buildAssertionInfo(final boolean isCron) {
    return new AssertionInfo()
        .setType(AssertionType.FRESHNESS)
        .setFreshnessAssertion(
            new FreshnessAssertionInfo()
                .setEntity(DATASET_URN)
                .setSchedule(
                    isCron
                        ? new FreshnessAssertionSchedule()
                            .setType(FreshnessAssertionScheduleType.CRON)
                            .setCron(
                                new FreshnessCronSchedule()
                                    .setCron("somecron")
                                    .setTimezone("sometz"))
                        : new FreshnessAssertionSchedule()
                            .setType(FreshnessAssertionScheduleType.FIXED_INTERVAL)
                            .setFixedInterval(
                                new FixedIntervalSchedule()
                                    .setMultiple(500)
                                    .setUnit(CalendarInterval.DAY))))
        .setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL));
  }
}
