package com.linkedin.gms.factory.telemetry;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;


@Slf4j
@Configuration
@EnableScheduling
public class ScheduledAnalyticsFactory {

    @Bean
    @ConditionalOnProperty("telemetry.enabledServer")
    public DailyReport dailyReport(@Qualifier("elasticSearchRestHighLevelClient") RestHighLevelClient elasticClient,
        @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN) IndexConvention indexConvention,
        ConfigurationProvider configurationProvider, EntityService entityService) {
        return new DailyReport(indexConvention, elasticClient, configurationProvider, entityService);
    }
}
