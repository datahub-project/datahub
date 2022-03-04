package com.linkedin.metadata.telemetry;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.gms.factory.search.ElasticSearchServiceFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.json.JSONObject;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Optional;

@Slf4j
@Configuration
@EnableScheduling
@RequiredArgsConstructor
public class ScheduledAnalyticsFactory {
    static Telemetry telemetry = new Telemetry();

    @Autowired
    private final AnalyticsService _analyticsService;

    @Scheduled(fixedDelay = 1000)
    @Bean
    public void reportWAU() {
//        telemetry.ping("test", new JSONObject());
//
//        DateTime endDate = DateTime.now();
//        DateTime startDate = endDate.minusWeeks(1);
//        DateRange dateRange = new DateRange(String.valueOf(startDate.getMillis()), String.valueOf(endDate.getMillis()));
//
//        int weeklyActiveUsers =
//                _analyticsService.getHighlights(_analyticsService.getUsageIndexName(), Optional.of(dateRange),
//                        ImmutableMap.of(), ImmutableMap.of(), Optional.of("browserId"));
//
//        log.info("WAU " + weeklyActiveUsers);

        log.info(
                "Fixed delay task - " + System.currentTimeMillis() / 1000);
    }
}
