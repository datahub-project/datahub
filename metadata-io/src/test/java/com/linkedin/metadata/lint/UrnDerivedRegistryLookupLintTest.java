package com.linkedin.metadata.lint;

import static org.testng.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.testng.annotations.Test;

/**
 * Ratchet against new {@code getEntitySpec(<something>.getEntityType())} lookups in server code.
 *
 * <p>An entity type taken from a urn comes from data, and after a zero-downtime upgrade rollback
 * that data can name entity types this version's registry doesn't know. {@code getEntitySpec}
 * throws for those, failing the whole request, batch or job. Use {@code
 * EntityRegistry.findEntitySpec} / {@code findAspectSpec} when the spec is needed, {@code
 * RegistryKnowledge} to classify, and {@code UnknownDataGuard} to skip with reporting.
 *
 * <p>Existing lookups are listed per file in {@code urn-derived-registry-lookups.baseline}. A file
 * may not gain lookups; when you remove some, lower its count in the baseline.
 */
public class UrnDerivedRegistryLookupLintTest {

  private static final List<String> MODULES =
      List.of(
          "metadata-io",
          "metadata-jobs",
          "metadata-service",
          "datahub-upgrade",
          "datahub-graphql-core");

  private static final Pattern LOOKUP =
      Pattern.compile("getEntitySpec\\([^;]*?getEntityType\\(\\)\\s*\\)", Pattern.DOTALL);

  private static final String BASELINE = "/urn-derived-registry-lookups.baseline";

  @Test
  public void testNoNewUrnDerivedGetEntitySpecLookups() throws IOException {
    final Map<String, Integer> baseline = readBaseline();
    final List<String> violations = new ArrayList<>();
    countLookups(repoRoot())
        .forEach(
            (file, count) -> {
              final int allowed = baseline.getOrDefault(file, 0);
              if (count > allowed) {
                violations.add(
                    String.format("%s: %d lookup(s), baseline %d", file, count, allowed));
              }
            });
    if (!violations.isEmpty()) {
      fail(
          "New getEntitySpec(<urn>.getEntityType()) lookups. They throw for entity types the "
              + "registry doesn't know (data from a newer version after a rollback). Use "
              + "findEntitySpec/findAspectSpec, RegistryKnowledge or UnknownDataGuard instead:\n  "
              + String.join("\n  ", violations));
    }
  }

  static Map<String, Integer> countLookups(final Path repoRoot) throws IOException {
    final Map<String, Integer> counts = new TreeMap<>();
    for (String module : MODULES) {
      try (Stream<Path> files = Files.walk(repoRoot.resolve(module))) {
        for (Path file :
            files
                .filter(p -> p.toString().endsWith(".java"))
                .filter(p -> p.toString().contains("/src/main/java/"))
                .filter(p -> !p.toString().contains("/build/"))
                .toList()) {
          final Matcher matcher = LOOKUP.matcher(Files.readString(file));
          int count = 0;
          while (matcher.find()) {
            count++;
          }
          if (count > 0) {
            counts.put(repoRoot.relativize(file).toString(), count);
          }
        }
      }
    }
    return counts;
  }

  private static Path repoRoot() {
    // Tests run from the module directory.
    return Paths.get("").toAbsolutePath().getParent();
  }

  private static Map<String, Integer> readBaseline() throws IOException {
    final Map<String, Integer> baseline = new TreeMap<>();
    try (InputStream in =
        Objects.requireNonNull(
            UrnDerivedRegistryLookupLintTest.class.getResourceAsStream(BASELINE))) {
      for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
        final String trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
          continue;
        }
        final int separator = trimmed.lastIndexOf(' ');
        baseline.put(
            trimmed.substring(0, separator), Integer.parseInt(trimmed.substring(separator + 1)));
      }
    }
    return baseline;
  }
}
