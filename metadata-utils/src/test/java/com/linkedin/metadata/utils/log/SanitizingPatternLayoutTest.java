package com.linkedin.metadata.utils.log;

import static org.testng.Assert.*;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SanitizingPatternLayoutTest {

  private SanitizingPatternLayout layout;

  @BeforeMethod
  public void setUp() {
    layout = new SanitizingPatternLayout();
    layout.setContext(new LoggerContext()); // ✅ Use LoggerContext instead of ContextBase
    layout.setPattern("%msg"); // Only log the message for testing
    layout.start();
  }

  private LoggingEvent createEvent(String msg) {
    return new LoggingEvent(
        "com.example.TestClass",
        (Logger) LoggerFactory.getLogger("TestLogger"),
        Level.INFO,
        msg,
        null,
        null);
  }

  @Test
  public void testNoSanitizationWhenDisabled() {
    setSanitizeEnabled(layout, false);
    String msg = "Hello\nWorld\tTest";
    String output = layout.doLayout(createEvent(msg));
    assertEquals(output, "Hello\nWorld\tTest", "Message should be unchanged when disabled");
  }

  @Test
  public void testSanitizeRemovesCarriageReturns() {
    setSanitizeEnabled(layout, true);
    String msg = "Hello\rWorld";
    String output = layout.doLayout(createEvent(msg));
    assertEquals(output, "Hello_World");
  }

  @Test
  public void testSanitizeReplacesTabsWithSpaces() {
    setSanitizeEnabled(layout, true);
    String msg = "Hello\tWorld";
    String output = layout.doLayout(createEvent(msg));
    assertEquals(output, "Hello    World");
  }

  @Test
  public void testSanitizeCollapsesMultipleNewlines() {
    setSanitizeEnabled(layout, true);
    String msg = "Line1\n\n\nLine2";
    String output = layout.doLayout(createEvent(msg));
    assertEquals(output, "Line1\nLine2");
  }

  @Test
  public void testSanitizeRemovesAnsiEscapeCodes() {
    setSanitizeEnabled(layout, true);
    String msg = "\u001B[31mRedText\u001B[0m Normal";
    String output = layout.doLayout(createEvent(msg));
    assertEquals(output, "RedText Normal");
  }

  @Test
  public void testNullMessage() {
    setSanitizeEnabled(layout, true);
    String output = layout.doLayout(createEvent(null));
    assert output != null;
    assertEquals(output, "null", "Null messages should be rendered as the string 'null'");
  }

  // Utility method to force sanitizeEnabled value for testing
  private void setSanitizeEnabled(SanitizingPatternLayout layout, boolean value) {
    try {
      var field = SanitizingPatternLayout.class.getDeclaredField("sanitizeEnabled");
      field.setAccessible(true);
      field.set(layout, value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
