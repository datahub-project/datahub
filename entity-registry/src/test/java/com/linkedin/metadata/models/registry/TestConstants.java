package com.linkedin.metadata.models.registry;

import org.apache.maven.artifact.versioning.ComparableVersion;


public class TestConstants {
  public static final String TEST_REGISTRY = "mycompany-dq-model";
  public static final String BASE_DIRECTORY = "custom-test-model/build/plugins/models";
  public static final ComparableVersion TEST_VERSION = new ComparableVersion("0.0.0-dev");
  public static final String TEST_ASPECT_NAME = "testDataQualityRules";
  public static final String TEST_EVENT_NAME = "dataQualityEvent";

  private TestConstants() {
  }
}
