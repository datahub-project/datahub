package com.linkedin.datahub.upgrade;

import static org.testng.Assert.*;

import com.linkedin.datahub.upgrade.impl.DefaultUpgradeReport;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpgradeReportTest {

  private UpgradeReport upgradeReport;

  @BeforeMethod
  public void setup() {
    upgradeReport = new DefaultUpgradeReport();
  }

  @Test
  public void testAddLine() {
    // Given
    String line1 = "Starting upgrade";
    String line2 = "Processing step 1";
    String line3 = "Upgrade completed";

    // When
    upgradeReport.addLine(line1);
    upgradeReport.addLine(line2);
    upgradeReport.addLine(line3);

    // Then
    List<String> lines = upgradeReport.lines();
    assertEquals(lines.size(), 3);
    assertEquals(lines.get(0), line1);
    assertEquals(lines.get(1), line2);
    assertEquals(lines.get(2), line3);
  }

  @Test
  public void testAddLineWithException() {
    // Given
    String errorMessage = "Error occurred during upgrade";
    Exception testException = new RuntimeException("Test exception message");

    // When
    upgradeReport.addLine(errorMessage, testException);

    // Then
    List<String> lines = upgradeReport.lines();
    assertEquals(lines.size(), 2);
    assertTrue(lines.get(0).contains(errorMessage));
    assertTrue(lines.get(0).contains("Test exception message"));
    assertTrue(lines.get(1).contains("Exception stack trace:"));
    assertTrue(lines.get(1).contains("RuntimeException"));
  }
}
