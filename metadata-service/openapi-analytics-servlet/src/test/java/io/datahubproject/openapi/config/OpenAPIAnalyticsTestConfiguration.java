package io.datahubproject.openapi.config;

import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import org.elasticsearch.action.search.SearchResponse;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.io.IOException;
import java.util.Optional;

import static io.datahubproject.openapi.delegates.DatahubUsageEventsImpl.DATAHUB_USAGE_INDEX;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@TestConfiguration
public class OpenAPIAnalyticsTestConfiguration {
    @Bean
    @Primary
    public ElasticSearchService datahubUsageEventsApiDelegate() throws IOException {
        ElasticSearchService elasticSearchService = mock(ElasticSearchService.class);
        SearchResponse mockResp = mock(SearchResponse.class);
        when(elasticSearchService.raw(eq(DATAHUB_USAGE_INDEX), anyString()))
                .thenReturn(Optional.of(mockResp));
        return elasticSearchService;
    }
}
