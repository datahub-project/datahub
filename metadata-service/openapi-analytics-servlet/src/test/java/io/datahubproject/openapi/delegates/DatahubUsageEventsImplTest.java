package io.datahubproject.openapi.delegates;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.PlatformAnalyticsConfiguration;
import com.linkedin.metadata.config.UsageEventsConfiguration;
import io.datahubproject.openapi.config.OpenAPIAnalyticsTestConfiguration;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.v2.generated.controller.DatahubUsageEventsApiController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(classes = {SpringWebConfig.class})
@ComponentScan(basePackages = {"io.datahubproject.openapi.v2.generated.controller"})
@Import({DatahubUsageEventsImpl.class, OpenAPIAnalyticsTestConfiguration.class})
public class DatahubUsageEventsImplTest extends AbstractTestNGSpringContextTests {

  @Autowired private DatahubUsageEventsApiController analyticsController;

  @MockitoBean private ConfigurationProvider configurationProvider;

  @MockitoBean private io.datahubproject.openapi.config.TracingInterceptor tracingInterceptor;

  // The @MockitoBean above replaces the @Primary ConfigurationProvider bean defined in
  // OpenAPIAnalyticsTestConfiguration with a fresh, unstubbed mock, so it must be re-stubbed
  // here to avoid NPEs in DatahubUsageEventsImpl#raw when it reads getPlatformAnalytics().
  @BeforeMethod
  public void setUp() {
    PlatformAnalyticsConfiguration platformAnalytics = mock(PlatformAnalyticsConfiguration.class);
    UsageEventsConfiguration usageEvents = mock(UsageEventsConfiguration.class);
    when(usageEvents.usePostgresql()).thenReturn(false);
    when(platformAnalytics.getUsageEvents()).thenReturn(usageEvents);
    when(configurationProvider.getPlatformAnalytics()).thenReturn(platformAnalytics);
  }

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
