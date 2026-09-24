package com.linkedin.datahub.upgrade.config;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.linkedin.datahub.upgrade.system.cdc.CDCSourceSetup;
import java.util.Collections;
import java.util.List;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CDCSetupConfigTest {

  private CDCSetupConfig cdcSetupConfig;
  private Logger configLogger;
  private ListAppender<ILoggingEvent> logAppender;

  @BeforeMethod
  public void setUp() {
    cdcSetupConfig = new CDCSetupConfig();
    configLogger = (Logger) LoggerFactory.getLogger(CDCSetupConfig.class);
    configLogger.setLevel(Level.INFO);
    logAppender = new ListAppender<>();
    logAppender.start();
    configLogger.addAppender(logAppender);
  }

  @AfterMethod
  public void tearDown() {
    configLogger.detachAppender(logAppender);
  }

  @Test
  public void testLogCdcSetupsWithNullList() {
    cdcSetupConfig.logCdcSetups(null);
    assertTrue(
        logAppender.list.stream()
            .anyMatch(
                e ->
                    e.getLevel() == Level.INFO
                        && e.getFormattedMessage().contains("No CDC source setups found")));
  }

  @Test
  public void testLogCdcSetupsWithEmptyList() {
    cdcSetupConfig.logCdcSetups(Collections.emptyList());
    assertTrue(
        logAppender.list.stream()
            .anyMatch(
                e ->
                    e.getLevel() == Level.INFO
                        && e.getFormattedMessage().contains("No CDC source setups found")));
  }

  @Test
  public void testLogCdcSetupsWithSingleImplementation() {
    CDCSourceSetup setup = mock(CDCSourceSetup.class);
    when(setup.id()).thenReturn("DebeziumCDCSetup");
    cdcSetupConfig.logCdcSetups(List.of(setup));
    assertTrue(
        logAppender.list.stream()
            .anyMatch(
                e ->
                    e.getLevel() == Level.INFO
                        && e.getFormattedMessage().contains("CDC source setup enabled")
                        && e.getFormattedMessage().contains("DebeziumCDCSetup")));
  }

  @Test
  public void testLogCdcSetupsWithMultipleImplementations() {
    CDCSourceSetup setup1 = mock(CDCSourceSetup.class);
    CDCSourceSetup setup2 = mock(CDCSourceSetup.class);
    when(setup1.id()).thenReturn("DebeziumCDCSetup");
    when(setup2.id()).thenReturn("OtherCDCSetup");
    cdcSetupConfig.logCdcSetups(List.of(setup1, setup2));
    assertTrue(
        logAppender.list.stream()
            .anyMatch(
                e ->
                    e.getLevel() == Level.WARN
                        && e.getFormattedMessage().contains("Multiple CDC source setups detected")
                        && e.getFormattedMessage().contains("DebeziumCDCSetup")
                        && e.getFormattedMessage().contains("OtherCDCSetup")));
  }
}
