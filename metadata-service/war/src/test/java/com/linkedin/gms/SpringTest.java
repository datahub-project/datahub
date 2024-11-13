package com.linkedin.gms;

import static org.testng.AssertJUnit.assertNotNull;

import com.linkedin.gms.factory.telemetry.DailyReport;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.ebean.Database;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.MOCK,
    properties = {"telemetry.enabledServer=true"})
@ContextConfiguration(classes = CommonApplicationConfig.class)
public class SpringTest extends AbstractTestNGSpringContextTests {

  // Mock Beans take precedence, we add these to avoid needing to configure data sources etc. while
  // still testing prod config
  @MockBean private Database database;
  @MockBean private ConfigEntityRegistry configEntityRegistry;
  @MockBean private EntityRegistry entityRegistry;

  @Test
  public void testTelemetry() {
    DailyReport dailyReport = this.applicationContext.getBean(DailyReport.class);
    assertNotNull(dailyReport);
  }
}
