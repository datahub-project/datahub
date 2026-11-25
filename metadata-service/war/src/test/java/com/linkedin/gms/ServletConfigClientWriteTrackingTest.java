package com.linkedin.gms;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.gms.factory.clientwrite.ClientWriteRecorder;
import com.linkedin.gms.factory.clientwrite.ClientWriteTrackingFilter;
import com.linkedin.metadata.config.ClientWriteTrackingConfiguration;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.testng.annotations.Test;

public class ServletConfigClientWriteTrackingTest {

  private static FilterRegistrationBean<ClientWriteTrackingFilter> registration(boolean enabled) {
    ClientWriteTrackingConfiguration configuration = new ClientWriteTrackingConfiguration();
    configuration.setEnabled(enabled);
    return new ServletConfig()
        .clientWriteTrackingFilterRegistration(
            mock(ClientWriteRecorder.class),
            configuration,
            TestOperationContexts.systemContextNoSearchAuthorization());
  }

  @Test
  public void testFilterIsRegisteredOnlyWhenTrackingIsEnabled() {
    assertTrue(registration(true).isEnabled());
    assertFalse(registration(false).isEnabled());
  }
}
