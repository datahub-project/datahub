package com.linkedin.metadata.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.jobs.common.health.kafka.KafkaHealthIndicator;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = {MceConsumerApplication.class, MceConsumerApplicationTestConfiguration.class},
    properties = {"authentication.enabled=false"})
public class MceConsumerApplicationTest extends AbstractTestNGSpringContextTests {

  @Autowired private TestRestTemplate restTemplate;

  @Autowired private EntityService<?> _mockEntityService;

  @Autowired private KafkaHealthIndicator kafkaHealthIndicator;

  @Test
  public void testRestliServletConfig() {
    RestoreIndicesResult mockResult = new RestoreIndicesResult();
    mockResult.setRowsMigrated(100);
    when(_mockEntityService.restoreIndices(any(OperationContext.class), any(), any()))
        .thenReturn(List.of(mockResult));

    String response =
        this.restTemplate.postForObject(
            "/gms/aspects?action=restoreIndices", "{\"urn\":\"\"}", String.class);
    assertTrue(response.contains(mockResult.toString()), String.format("Found: %s", response));
  }

  @Test
  public void testHealthIndicator() {
    assertNotNull(kafkaHealthIndicator);
  }
}
