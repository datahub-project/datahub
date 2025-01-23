package io.datahubproject.openapi.delegates;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import io.datahubproject.openapi.config.OpenAPIAnalyticsTestConfiguration;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.v2.generated.controller.DatahubUsageEventsApiController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(classes = {SpringWebConfig.class})
@ComponentScan(basePackages = {"io.datahubproject.openapi.v2.generated.controller"})
@Import({DatahubUsageEventsImpl.class, OpenAPIAnalyticsTestConfiguration.class})
public class DatahubUsageEventsImplTest extends AbstractTestNGSpringContextTests {

  @Autowired private DatahubUsageEventsApiController analyticsController;

  @MockBean private ConfigurationProvider configurationProvider;

  @Test
  public void initTest() {
    assertNotNull(analyticsController);
  }

  @Test
  public void analyticsControllerTest() {
    ResponseEntity<String> resp = analyticsController.raw("");
    assertEquals(resp.getStatusCode(), HttpStatus.OK);
  }
}
