package io.datahubproject.telemetry;

import com.linkedin.gms.factory.telemetry.TelemetryUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.telemetry.TelemetryClientId;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;


public class TelemetryUtilsTest {

  EntityService _entityService;

  @BeforeMethod
  public void init() {
    _entityService = Mockito.mock(EntityService.class);
    Mockito.when(_entityService.getLatestAspect(any(), anyString())).thenReturn(new TelemetryClientId().setClientId("1234"));
  }

  @Test
  public void getClientIdTest() {
    assertEquals("1234", TelemetryUtils.getClientId(_entityService));
  }
}
