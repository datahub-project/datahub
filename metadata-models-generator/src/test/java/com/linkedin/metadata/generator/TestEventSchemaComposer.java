package com.linkedin.metadata.generator;

import java.io.File;
import java.io.FileInputStream;
import org.apache.commons.io.IOUtils;
import org.rythmengine.Rythm;
import org.testng.annotations.Test;

import static com.linkedin.metadata.generator.SchemaGeneratorConstants.*;
import static com.linkedin.metadata.generator.TestEventSpec.*;
import static org.testng.Assert.*;


public class TestEventSchemaComposer {

  private static final String GENERATED_MXE_PATH = "src/testGeneratedPdl/pegasus/com/linkedin/mxe";
  private static final String TEST_NAMESPACE = "/bar/annotatedAspectBar";
  private static final String TEST_GENERATED_PDL = "MetadataChangeEvent.pdl";

  @Test
  public void testMCESchemaRender() throws Exception {
    final String testMCE = GENERATED_MXE_PATH + TEST_NAMESPACE + File.separator + TEST_GENERATED_PDL;
    final File metadataChangeEvent = new File(testMCE);

    populateEvents();

    assertTrue(metadataChangeEvent.exists());
    assertEquals(IOUtils.toString(new FileInputStream(testMCE)), IOUtils.toString(this.getClass()
        .getClassLoader()
        .getResourceAsStream("com/linkedin/mxe" + TEST_NAMESPACE + File.separator + TEST_GENERATED_PDL)));
  }

  @Test
  public void testFMCESchemaRender() throws Exception {
    final String testFMCE =
        GENERATED_MXE_PATH + TEST_NAMESPACE + File.separator + FAILED_METADATA_CHANGE_EVENT_PREFIX + TEST_GENERATED_PDL;
    final File failedMetadataChangeEventBar = new File(testFMCE);

    populateEvents();

    assertTrue(failedMetadataChangeEventBar.exists());
    assertEquals(IOUtils.toString(new FileInputStream(testFMCE)), IOUtils.toString(this.getClass()
        .getClassLoader()
        .getResourceAsStream("com/linkedin/mxe" + TEST_NAMESPACE + File.separator + FAILED_METADATA_CHANGE_EVENT_PREFIX
            + TEST_GENERATED_PDL)));
  }

  @Test
  public void testMAESchemaRender() throws Exception {
    final String testMAE =
        GENERATED_MXE_PATH + TEST_NAMESPACE + File.separator + METADATA_AUDIT_EVENT + PDL_SUFFIX;
    final File metadataAuditEventBar = new File(testMAE);

    populateEvents();

    assertTrue(metadataAuditEventBar.exists());
    assertEquals(IOUtils.toString(new FileInputStream(testMAE)), IOUtils.toString(this.getClass()
        .getClassLoader()
        .getResourceAsStream(
            "com/linkedin/mxe" + TEST_NAMESPACE + File.separator + METADATA_AUDIT_EVENT + PDL_SUFFIX)));
  }

  private void populateEvents() throws Exception {
    SchemaAnnotationRetriever schemaAnnotationRetriever =
        new SchemaAnnotationRetriever(TEST_METADATA_MODELS_RESOLVED_PATH);
    final String[] sources = {TEST_METADATA_MODELS_SOURCE_PATH};
    EventSchemaComposer eventSchemaComposer = new EventSchemaComposer();
    eventSchemaComposer.setupRythmEngine();
    eventSchemaComposer.render(schemaAnnotationRetriever.generate(sources), GENERATED_MXE_PATH);
    Rythm.shutdown();
  }
}