package io.datahubproject.openapi.delegates;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.datahub.authorization.AuthorizerChain;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.config.OpenAPIAnalyticsTestConfiguration;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.v2.generated.controller.DatahubUsageEventsApiController;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(classes = {SpringWebConfig.class})
@ComponentScan(basePackages = {"io.datahubproject.openapi.v2.generated.controller"})
@Import({DatahubUsageEventsImpl.class, OpenAPIAnalyticsTestConfiguration.class})
public class DatahubUsageEventsImplTest extends AbstractTestNGSpringContextTests {

  @Autowired private DatahubUsageEventsApiController analyticsController;
  @Autowired private AuthorizerChain authorizerChain;

  @Autowired
  @Qualifier("systemOperationContext")
  private OperationContext systemOperationContext;

  @MockitoBean private ConfigurationProvider configurationProvider;

  @MockitoBean private io.datahubproject.openapi.config.TracingInterceptor tracingInterceptor;

  @Test
  public void initTest() {
    assertNotNull(analyticsController);
  }

  @Test
  public void analyticsControllerTest() {
    ResponseEntity<String> resp = analyticsController.raw("");
    assertEquals(resp.getStatusCode(), HttpStatus.OK);
  }

  @Test
  public void rawReturnsNotImplementedWithoutElasticsearch() {
    HttpServletRequest request = org.mockito.Mockito.mock(HttpServletRequest.class);
    DatahubUsageEventsImpl impl =
        new DatahubUsageEventsImpl(null, authorizerChain, systemOperationContext, request);
    ResponseEntity<String> resp = impl.raw("{}");
    assertEquals(resp.getStatusCode(), HttpStatus.NOT_IMPLEMENTED);
  }
}
