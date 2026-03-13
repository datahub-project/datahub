package com.linkedin.metadata.utils.log;

import static org.testng.Assert.assertEquals;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.spi.FilterReply;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LogMessageFilterTest {

  private LogMessageFilter filter;

  @BeforeMethod
  public void setUp() {
    filter = new LogMessageFilter();
    filter.addExcluded("noisy message");
    filter.start();
  }

  @Test
  public void testNullFormattedMessage() {
    ILoggingEvent event = Mockito.mock(ILoggingEvent.class);
    Mockito.when(event.getFormattedMessage()).thenReturn(null);
    Mockito.when(event.getThrowableProxy()).thenReturn(null);

    // Should not throw NPE — should accept since message can't match exclusions
    assertEquals(filter.decide(event), FilterReply.ACCEPT);
  }

  @Test
  public void testExcludedMessageIsDenied() {
    ILoggingEvent event = Mockito.mock(ILoggingEvent.class);
    Mockito.when(event.getFormattedMessage()).thenReturn("this is a noisy message from kafka");
    Mockito.when(event.getThrowableProxy()).thenReturn(null);

    assertEquals(filter.decide(event), FilterReply.DENY);
  }

  @Test
  public void testNonExcludedMessageIsAccepted() {
    ILoggingEvent event = Mockito.mock(ILoggingEvent.class);
    Mockito.when(event.getFormattedMessage()).thenReturn("normal log line");
    Mockito.when(event.getThrowableProxy()).thenReturn(null);

    assertEquals(filter.decide(event), FilterReply.ACCEPT);
  }

  @Test
  public void testNullFormattedMessageWithExcludedThrowableIsDenied() {
    // formattedMessage is null but the excluded string appears in the throwable —
    // the null guard on formattedMessage must not prevent checking the throwable
    ILoggingEvent event = Mockito.mock(ILoggingEvent.class);
    Mockito.when(event.getFormattedMessage()).thenReturn(null);

    IThrowableProxy throwableProxy = Mockito.mock(IThrowableProxy.class);
    Mockito.when(throwableProxy.getClassName()).thenReturn("java.lang.RuntimeException");
    Mockito.when(throwableProxy.getMessage()).thenReturn("this is a noisy message from somewhere");
    Mockito.when(throwableProxy.getCause()).thenReturn(null);
    Mockito.when(throwableProxy.getSuppressed()).thenReturn(new IThrowableProxy[0]);
    Mockito.when(throwableProxy.getCommonFrames()).thenReturn(0);
    Mockito.when(throwableProxy.getStackTraceElementProxyArray())
        .thenReturn(new ch.qos.logback.classic.spi.StackTraceElementProxy[0]);
    Mockito.when(event.getThrowableProxy()).thenReturn(throwableProxy);

    assertEquals(filter.decide(event), FilterReply.DENY);
  }

  @Test
  public void testFilterNotStartedReturnsNeutral() {
    LogMessageFilter unstartedFilter = new LogMessageFilter();
    unstartedFilter.addExcluded("something");
    // NOT calling start()

    ILoggingEvent event = Mockito.mock(ILoggingEvent.class);
    Mockito.when(event.getFormattedMessage()).thenReturn("something");

    assertEquals(unstartedFilter.decide(event), FilterReply.NEUTRAL);
  }
}
