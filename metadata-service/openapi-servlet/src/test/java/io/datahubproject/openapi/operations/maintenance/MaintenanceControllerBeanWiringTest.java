package io.datahubproject.openapi.operations.maintenance;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;

import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * Integration test that verifies MaintenanceController can be properly wired when both entityClient
 * and systemEntityClient beans exist (as in production).
 *
 * <p>This test catches @Qualifier annotation issues that unit tests with single mock beans cannot
 * detect.
 */
@SpringBootTest(classes = MaintenanceControllerBeanWiringTest.TestConfig.class)
public class MaintenanceControllerBeanWiringTest extends AbstractTestNGSpringContextTests {

  @Autowired private MaintenanceController maintenanceController;

  @Test
  public void testControllerWiresCorrectlyWithBothEntityClientBeans() {
    // If we reach here without NoUniqueBeanDefinitionException, the @Qualifier is working
    assertNotNull(
        maintenanceController,
        "MaintenanceController should be wired correctly when both entityClient "
            + "and systemEntityClient beans exist");
  }

  @SpringBootConfiguration
  @Import(TracingInterceptor.class)
  @ComponentScan(basePackages = {"io.datahubproject.openapi.operations.maintenance"})
  static class TestConfig {

    @Bean(name = "entityClient")
    public EntityClient entityClient() {
      return mock(EntityClient.class);
    }

    @Bean(name = "systemEntityClient")
    public SystemEntityClient systemEntityClient() {
      return mock(SystemEntityClient.class);
    }

    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }

    @Bean
    public SystemTelemetryContext systemTelemetryContext() {
      return mock(SystemTelemetryContext.class);
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext(
        ObjectMapper objectMapper, SystemTelemetryContext systemTelemetryContext) {
      return TestOperationContexts.systemContextTraceNoSearchAuthorization(
          () -> ObjectMapperContext.builder().objectMapper(objectMapper).build(),
          () -> systemTelemetryContext);
    }

    @Bean
    public AuthorizerChain authorizerChain() {
      return mock(AuthorizerChain.class);
    }
  }
}
