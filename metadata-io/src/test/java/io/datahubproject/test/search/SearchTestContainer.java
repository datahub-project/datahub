package io.datahubproject.test.search;

import java.time.Duration;
import org.testcontainers.containers.GenericContainer;

public interface SearchTestContainer {

  String SEARCH_JAVA_OPTS = "-Xms512m -Xmx512m -XX:MaxDirectMemorySize=368435456";

  Duration STARTUP_TIMEOUT = Duration.ofMinutes(5); // usually < 1min

  GenericContainer<?> startContainer();

  void stopContainer();
}
