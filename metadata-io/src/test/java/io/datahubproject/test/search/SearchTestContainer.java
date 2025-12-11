/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.test.search;

import java.time.Duration;
import org.testcontainers.containers.GenericContainer;

public interface SearchTestContainer {

  // Read heap size from system property - set by build.gradle based on GRADLE_MEMORY_PROFILE
  String ELASTICSEARCH_HEAP = System.getProperty("testcontainers.elasticsearch.heap", "512m");
  String SEARCH_JAVA_OPTS = "-Xms" + ELASTICSEARCH_HEAP + " -Xmx" + ELASTICSEARCH_HEAP;

  Duration STARTUP_TIMEOUT = Duration.ofMinutes(5); // usually < 1min

  GenericContainer<?> startContainer();

  void stopContainer();
}
