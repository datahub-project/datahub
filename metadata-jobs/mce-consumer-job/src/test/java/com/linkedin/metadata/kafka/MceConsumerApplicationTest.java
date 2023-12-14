package com.linkedin.metadata.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertTrue;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = {MceConsumerApplication.class, MceConsumerApplicationTestConfiguration.class})
public class MceConsumerApplicationTest extends AbstractTestNGSpringContextTests {

  @Autowired private TestRestTemplate restTemplate;

  @Autowired private EntityService _mockEntityService;

  @Test
  public void testRestliServletConfig() {
    RestoreIndicesResult mockResult = new RestoreIndicesResult();
    mockResult.setRowsMigrated(100);
    when(_mockEntityService.restoreIndices(any(), any())).thenReturn(mockResult);

    String response =
        this.restTemplate.postForObject(
            "/gms/aspects?action=restoreIndices", "{\"urn\":\"\"}", String.class);
    assertTrue(response.contains(mockResult.toString()));
  }
}
