package com.linkedin.datahub.upgrade.config;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.datahub.upgrade.system.cdc.CDCSourceSetup;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CDCSetupConfigTest {

  private CDCSetupConfig cdcSetupConfig;

  @BeforeMethod
  public void setUp() {
    cdcSetupConfig = new CDCSetupConfig();
  }

  @Test
  public void testLogCdcSetupsWithNullList() {
    cdcSetupConfig.logCdcSetups(null);
  }

  @Test
  public void testLogCdcSetupsWithEmptyList() {
    cdcSetupConfig.logCdcSetups(Collections.emptyList());
  }

  @Test
  public void testLogCdcSetupsWithSingleImplementation() {
    CDCSourceSetup setup = mock(CDCSourceSetup.class);
    when(setup.id()).thenReturn("DebeziumCDCSetup");
    cdcSetupConfig.logCdcSetups(List.of(setup));
  }

  @Test
  public void testLogCdcSetupsWithMultipleImplementations() {
    CDCSourceSetup setup1 = mock(CDCSourceSetup.class);
    CDCSourceSetup setup2 = mock(CDCSourceSetup.class);
    when(setup1.id()).thenReturn("DebeziumCDCSetup");
    when(setup2.id()).thenReturn("OtherCDCSetup");
    cdcSetupConfig.logCdcSetups(List.of(setup1, setup2));
  }
}
