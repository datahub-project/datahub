package com.linkedin.gms;

import static org.testng.AssertJUnit.assertNotNull;

import com.linkedin.gms.factory.telemetry.DailyReport;
import com.linkedin.metadata.boot.BootstrapManager;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.MOCK,
    properties = {
      "telemetry.enabledServer=true",
      "spring.main.allow-bean-definition-overriding=true"
    })
@ContextConfiguration(classes = {CommonApplicationConfig.class, SpringTest.TestBeans.class})
public class SpringTest extends AbstractTestNGSpringContextTests {

  // Mock Beans take precedence, we add these to avoid needing to configure data sources etc. while
  // still testing prod config
  @MockBean private Database database;
  @MockBean private BootstrapManager bootstrapManager;

  @Test
  public void testTelemetry() {
    DailyReport dailyReport = this.applicationContext.getBean(DailyReport.class);
    assertNotNull(dailyReport);
  }

  @TestConfiguration
  public static class TestBeans {
    @Bean
    public OperationContext systemOperationContext() {
      return TestOperationContexts.systemContextNoSearchAuthorization();
    }

    @Primary
    @Bean
    public EntityRegistry entityRegistry(OperationContext systemOperationContext) {
      return systemOperationContext.getEntityRegistry();
    }
  }
}
