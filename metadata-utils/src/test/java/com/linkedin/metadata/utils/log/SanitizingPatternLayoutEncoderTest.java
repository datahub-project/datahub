package com.linkedin.metadata.utils.log;

import static org.testng.Assert.*;

import ch.qos.logback.classic.LoggerContext;
import org.testng.annotations.Test;

public class SanitizingPatternLayoutEncoderTest {

  @Test
  public void testStartCoversAllLines() {
    // Arrange
    SanitizingPatternLayoutEncoder encoder = new SanitizingPatternLayoutEncoder();
    encoder.setContext(new LoggerContext());
    encoder.setPattern("%msg");

    // Act
    encoder.start();

    // Assert
    assertNotNull(encoder.getLayout(), "Layout should be initialized");
  }
}
