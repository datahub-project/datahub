/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.models.registry;

import org.apache.maven.artifact.versioning.ComparableVersion;

public class TestConstants {
  public static final String TEST_REGISTRY = "mycompany-dq-model";
  public static final String BASE_DIRECTORY = "custom-test-model/build/plugins/models";
  public static final ComparableVersion TEST_VERSION = new ComparableVersion("0.0.0-dev");
  public static final String TEST_ASPECT_NAME = "testDataQualityRules";
  public static final String TEST_EVENT_NAME = "dataQualityEvent";

  private TestConstants() {}
}
