package io.datahubproject.openapi.metrics;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.prometheus.client.exporter.common.TextFormat;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@AllArgsConstructor
@RequestMapping("/metrics")
@Slf4j
public class MicrometerController {

    @GetMapping(value = "/prometheus")
    public ResponseEntity<String> getPrometheusData() {
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, TextFormat.CONTENT_TYPE_004)
                .body(MetricUtils.scrapePrometheusData());
    }

}