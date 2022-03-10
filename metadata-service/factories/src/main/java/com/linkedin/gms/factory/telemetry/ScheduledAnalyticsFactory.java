package com.linkedin.gms.factory.telemetry;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.elasticsearch.client.RestHighLevelClient;
import org.joda.time.DateTime;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Optional;

@Slf4j
@Configuration
@EnableScheduling
public class ScheduledAnalyticsFactory {
    @Autowired
    @Qualifier("elasticSearchRestHighLevelClient")
    private RestHighLevelClient elasticClient;

    @Autowired
    @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
    private IndexConvention indexConvention;

    // statistics to send daily
    @Scheduled(fixedDelay = 24 * 60 * 60 * 1000)
    public void dailyReport() {
        AnalyticsService analyticsService = new AnalyticsService(elasticClient, indexConvention);

        DateTime endDate = DateTime.now();
        DateTime yesterday = endDate.minusDays(1);
        DateTime lastWeek = endDate.minusWeeks(1);
        DateTime lastMonth = endDate.minusMonths(1);

        DateRange dayRange = new DateRange(String.valueOf(yesterday.getMillis()), String.valueOf(endDate.getMillis()));
        DateRange weekRange = new DateRange(String.valueOf(lastWeek.getMillis()), String.valueOf(endDate.getMillis()));
        DateRange monthRange = new DateRange(String.valueOf(lastMonth.getMillis()), String.valueOf(endDate.getMillis()));

        int dailyActiveUsers =
                analyticsService.getHighlights(analyticsService.getUsageIndexName(), Optional.of(dayRange),
                        ImmutableMap.of(), ImmutableMap.of(), Optional.of("browserId"));
        int weeklyActiveUsers =
                analyticsService.getHighlights(analyticsService.getUsageIndexName(), Optional.of(weekRange),
                        ImmutableMap.of(), ImmutableMap.of(), Optional.of("browserId"));
        int monthlyActiveUsers =
                analyticsService.getHighlights(analyticsService.getUsageIndexName(), Optional.of(monthRange),
                        ImmutableMap.of(), ImmutableMap.of(), Optional.of("browserId"));

        // floor to nearest power of 10
        dailyActiveUsers = (int) Math.pow(10, (int) Math.log10(dailyActiveUsers + 1));
        weeklyActiveUsers = (int) Math.pow(10, (int) Math.log10(weeklyActiveUsers + 1));
        monthlyActiveUsers = (int) Math.pow(10, (int) Math.log10(monthlyActiveUsers + 1));

        // set user-level properties
        JSONObject report = new JSONObject();
        report.put("dau", dailyActiveUsers);
        report.put("wau", weeklyActiveUsers);
        report.put("mau", monthlyActiveUsers);

        Telemetry.ping("service-daily", report);
    }
}
