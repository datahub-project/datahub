package com.linkedin.metadata.examples.cli;

import com.linkedin.restli.common.ContentType;
import java.io.File;
import java.io.FileInputStream;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Simple test to help us keep our example file up to date with the MCE schema definition, in the event we change
 * schemas, or change the file without manually testing it (which we shouldn't do, but can happen by mistake).
 */
public class TestExamples {
  private static final File EXAMPLE_FILE = new File("example-bootstrap.json");

  @Test
  public void examplesAreValidJson() throws Exception {
    assertTrue(EXAMPLE_FILE.exists());
    // no exception = test passes
    ContentType.JSON.getCodec().readMap(new FileInputStream(EXAMPLE_FILE));
  }

  @Test
  public void examplesMatchSchemas() throws Exception {
    // no exception = test passes
    MceCli.readEventsFile(EXAMPLE_FILE);
  }
}
