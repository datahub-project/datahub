package io.datahubproject.openapi.delegates;

import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import io.datahubproject.openapi.generated.controller.DatahubUsageEventsApiDelegate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;

import java.util.Objects;

public class DatahubUsageEventsImpl implements DatahubUsageEventsApiDelegate {

    @Autowired
    private ElasticSearchService _searchService;

    final public static String DATAHUB_USAGE_INDEX = "datahub_usage_event";

    @Override
    public ResponseEntity<String> raw(String body) {
        return ResponseEntity.of(_searchService.raw(DATAHUB_USAGE_INDEX, body).map(Objects::toString));
    }
}
