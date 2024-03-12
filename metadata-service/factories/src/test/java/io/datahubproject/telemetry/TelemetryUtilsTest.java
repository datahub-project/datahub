package io.datahubproject.telemetry;

import static org.mockito.ArgumentMatchers.*;
import static org.testng.AssertJUnit.assertEquals;

import com.linkedin.gms.factory.telemetry.TelemetryUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.telemetry.TelemetryClientId;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TelemetryUtilsTest {

  EntityService _entityService;

  @BeforeMethod
  public void init() {
    _entityService = Mockito.mock(EntityService.class);
    Mockito.when(_entityService.getLatestAspect(any(), anyString()))
        .thenReturn(new TelemetryClientId().setClientId("1234"));
  }

  @Test
  public void getClientIdTest() {
    assertEquals("1234", TelemetryUtils.getClientId(_entityService));
  }
}
