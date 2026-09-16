package io.datahubproject.openlineage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.FabricType;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.testng.annotations.Test;

/**
 * Golden comparison of everything the converter emits for the vendored compatibility corpus.
 *
 * <p>{@link CompatibilityCorpusTest} names a handful of facets and asserts they reach their
 * aspects, which says what we meant. This says what we actually produce: every proposal, every URN,
 * every field. A mapping that starts emitting a subtly different field path or a wrong platform
 * shows up here even though nobody thought to assert it.
 *
 * <p>Regenerate after an intentional change:
 *
 * <pre>
 *   ./gradlew :metadata-integration:java:openlineage-converter:test \
 *       --tests "*CompatibilityGoldenTest*" -Dopenlineage.golden.regenerate=true
 * </pre>
 *
 * and read the diff — a golden nobody reads is a golden that stops catching anything.
 */
public class CompatibilityGoldenTest {

  private static final String REGENERATE = "openlineage.golden.regenerate";
  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  /**
   * Edge audit stamps are built from the wall clock, so they differ on every run. Times the event
   * itself supplied are left alone: those are worth asserting. A day of slack separates the two
   * without needing to know which field is which.
   */
  private static final long NOW_WINDOW_MILLIS = 24L * 60 * 60 * 1000;

  @Test
  public void convertedEventsMatchTheirGoldens() throws Exception {
    boolean regenerate = Boolean.getBoolean(REGENERATE);
    long now = System.currentTimeMillis();
    List<String> mismatches = new ArrayList<>();
    int compared = 0;

    for (File event : corpus()) {
      String rel = relative(event);
      if (rel.equals(CompatibilityCorpusTest.NON_CONFORMANT)) {
        continue;
      }
      String actual = render(read(event), now);
      File golden = new File(goldenRoot(), rel);

      if (regenerate) {
        golden.getParentFile().mkdirs();
        Files.write(golden.toPath(), actual.getBytes(StandardCharsets.UTF_8));
        continue;
      }
      if (!golden.exists()) {
        mismatches.add(rel + ": no golden file; regenerate with -D" + REGENERATE + "=true");
        continue;
      }
      String expected = read(golden);
      if (!expected.equals(actual)) {
        mismatches.add(rel + ":\n" + firstDifference(expected, actual));
      }
      compared++;
    }

    if (regenerate) {
      throw new AssertionError(
          "goldens regenerated; re-run without -D" + REGENERATE + " and review the diff");
    }
    assertTrue(mismatches.isEmpty(), "golden mismatches:\n" + String.join("\n\n", mismatches));
    assertEquals(compared, 73, "corpus size changed; re-check the vendored events");
  }

  /** Canonical, readable rendering: aspects parsed rather than embedded as escaped strings. */
  private static String render(String body, long now) throws Exception {
    OpenLineage.RunEvent event = OpenLineageClientUtils.runEventFromJson(body);
    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());

    ArrayNode out = MAPPER.createArrayNode();
    for (MetadataChangeProposal m : mcps) {
      ObjectNode node = MAPPER.createObjectNode();
      node.put("entityType", m.getEntityType());
      node.put("entityUrn", m.getEntityUrn() == null ? null : m.getEntityUrn().toString());
      node.put("aspectName", m.getAspectName());
      node.put("changeType", String.valueOf(m.getChangeType()));
      if (m.getAspect() != null) {
        node.set(
            "aspect",
            blankWallClockTimes(
                MAPPER.readTree(m.getAspect().getValue().asString(StandardCharsets.UTF_8)), now));
      }
      out.add(node);
    }
    return MAPPER.writeValueAsString(out) + "\n";
  }

  private static JsonNode blankWallClockTimes(JsonNode node, long now) {
    if (node.isObject()) {
      ObjectNode obj = (ObjectNode) node;
      obj.fieldNames()
          .forEachRemaining(
              name -> {
                JsonNode child = obj.get(name);
                if (child.isNumber() && isWallClock(child.asLong(), now)) {
                  obj.put(name, "<WALL_CLOCK>");
                } else {
                  blankWallClockTimes(child, now);
                }
              });
    } else if (node.isArray()) {
      node.forEach(child -> blankWallClockTimes(child, now));
    }
    return node;
  }

  private static boolean isWallClock(long value, long now) {
    return Math.abs(now - value) < NOW_WINDOW_MILLIS;
  }

  private static String firstDifference(String expected, String actual) {
    String[] e = expected.split("\n");
    String[] a = actual.split("\n");
    for (int i = 0; i < Math.max(e.length, a.length); i++) {
      String le = i < e.length ? e[i] : "<missing>";
      String la = i < a.length ? a[i] : "<missing>";
      if (!le.equals(la)) {
        return "  line "
            + (i + 1)
            + "\n    expected: "
            + le.trim()
            + "\n    actual:   "
            + la.trim();
      }
    }
    return "  files differ only in trailing content";
  }

  private static String read(File f) throws Exception {
    return new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
  }

  private static File corpusRoot() throws Exception {
    return new File(
        CompatibilityGoldenTest.class.getClassLoader().getResource("compatibility").toURI());
  }

  /** Resolved from the corpus location so regeneration writes into the source tree, not build/. */
  private static File goldenRoot() throws Exception {
    File resources =
        corpusRoot()
            .toPath()
            .resolveSibling("../../../src/test/resources/compatibility-golden")
            .normalize()
            .toFile();
    return resources;
  }

  private static String relative(File f) {
    try {
      return corpusRoot().toURI().relativize(f.toURI()).getPath();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static List<File> corpus() throws Exception {
    List<File> out = new ArrayList<>();
    collect(corpusRoot(), out);
    out.sort(Comparator.comparing(File::getPath));
    return out;
  }

  private static void collect(File dir, List<File> out) {
    File[] kids = dir.listFiles();
    if (kids == null) {
      return;
    }
    for (File k : kids) {
      if (k.isDirectory()) {
        collect(k, out);
      } else if (k.getName().endsWith(".json")) {
        out.add(k);
      }
    }
  }

  private static DatahubOpenlineageConfig config() {
    return DatahubOpenlineageConfig.builder()
        .fabricType(FabricType.PROD)
        .materializeDataset(true)
        .includeSchemaMetadata(true)
        .captureColumnLevelLineage(true)
        .build();
  }
}
