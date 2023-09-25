package io.datahubproject.test.search;

import org.testcontainers.containers.GenericContainer;

import java.time.Duration;

public interface SearchTestContainer {
    String SEARCH_JAVA_OPTS = "-Xms64m -Xmx384m -XX:MaxDirectMemorySize=368435456";
    Duration STARTUP_TIMEOUT = Duration.ofMinutes(5); // usually < 1min

    GenericContainer<?> startContainer();

    void stopContainer();
}
