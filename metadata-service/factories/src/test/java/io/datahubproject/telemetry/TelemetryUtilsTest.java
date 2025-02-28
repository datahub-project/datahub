package io.datahubproject.telemetry;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;

import com.linkedin.gms.factory.telemetry.TelemetryUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.telemetry.TelemetryClientId;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TelemetryUtilsTest {

  EntityService<?> _entityService;

  @BeforeMethod
  public void init() {
    _entityService = mock(EntityService.class);
    Mockito.when(_entityService.getLatestAspect(any(OperationContext.class), any(), anyString()))
        .thenReturn(new TelemetryClientId().setClientId("1234"));
  }

  @Test
  public void getClientIdTest() {
    assertEquals("1234", TelemetryUtils.getClientId(mock(OperationContext.class), _entityService));
  }
}
