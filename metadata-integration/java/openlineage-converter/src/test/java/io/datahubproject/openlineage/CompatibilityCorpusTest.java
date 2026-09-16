package io.datahubproject.openlineage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataFlowUrn;
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
import java.util.stream.Collectors;
import org.testng.annotations.Test;

/**
 * Runs the OpenLineage project's own consumer compatibility corpus through the converter.
 *
 * <p>These are payloads real producers emitted -- Airflow, Spark on Dataproc, BigQuery -- rather
 * than events assembled with the OpenLineage Java builders. That distinction is the point: the rest
 * of this module's tests construct events in memory and never exercise deserialization, so a
 * mapping can be correct against a builder and still see nothing in a producer's actual JSON.
 *
 * <p>Corpus vendored from OpenLineage/compatibility-tests (Apache-2.0); see the README beside the
 * resources for provenance and the facet inventory.
 */
public class CompatibilityCorpusTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * One corpus event is a hand-written placeholder whose runId is the literal "run_id". The spec
   * requires a UUID and the OpenLineage client enforces it, so it is excluded here and asserted
   * separately.
   */
  private static final String NON_CONFORMANT = "simple_run_event/simple_run_event.json";

  @Test
  public void everyConformantEventConverts() throws Exception {
    List<String> failures = new ArrayList<>();
    int converted = 0;
    for (File f : corpus()) {
      String rel = relative(f);
      if (rel.equals(NON_CONFORMANT)) {
        continue;
      }
      try {
        List<MetadataChangeProposal> mcps = convert(read(f));
        assertFalse(mcps.isEmpty(), rel + " produced no proposals");
        converted++;
      } catch (Throwable t) {
        failures.add(rel + " -> " + t.getClass().getSimpleName() + ": " + t.getMessage());
      }
    }
    assertTrue(
        failures.isEmpty(),
        "events the converter could not handle:\n" + String.join("\n", failures));
    // Guards against the corpus silently emptying out, which would make this suite pass vacuously.
    assertEquals(converted, 73, "corpus size changed; re-check the vendored events");
  }

  /**
   * A malformed runId has to fail during deserialization, which is what lets the REST endpoint
   * answer 400 rather than 500 -- the caller sent something the spec does not allow.
   */
  @Test
  public void nonConformantRunIdFailsAsAClientError() throws Exception {
    File f =
        corpus().stream()
            .filter(x -> relative(x).equals(NON_CONFORMANT))
            .findFirst()
            .orElseThrow(() -> new AssertionError(NON_CONFORMANT + " is missing from the corpus"));
    try {
      convert(read(f));
      throw new AssertionError("a non-UUID runId should not have been accepted");
    } catch (AssertionError e) {
      throw e;
    } catch (Throwable expected) {
      assertTrue(
          String.valueOf(expected.getMessage()).contains("UUID"),
          "expected a UUID parse failure, got: " + expected);
    }
  }

  /**
   * The facets the corpus actually carries have to reach DataHub, not merely survive conversion.
   * Each case names a facet and the aspect it is supposed to become, so a mapping that silently
   * stops firing on real producer output fails here rather than in a user's catalog.
   */
  @Test
  public void mappedFacetsReachTheirAspects() throws Exception {
    assertFacetProducesAspect("columnLineage", "dataJobInputOutput", "fineGrainedLineages");
    assertFacetProducesAspect("schema", "schemaMetadata", null);
    assertFacetProducesAspect("lifecycleStateChange", "operation", null);
    assertFacetProducesAspect("outputStatistics", "operation", null);
  }

  private void assertFacetProducesAspect(String facet, String aspectName, String mustContain)
      throws Exception {
    List<File> carrying = new ArrayList<>();
    for (File f : corpus()) {
      if (!relative(f).equals(NON_CONFORMANT) && hasDatasetFacet(read(f), facet)) {
        carrying.add(f);
      }
    }
    assertFalse(carrying.isEmpty(), "no corpus event carries the " + facet + " facet any more");

    List<String> missing = new ArrayList<>();
    for (File f : carrying) {
      List<MetadataChangeProposal> mcps = convert(read(f));
      boolean found =
          mcps.stream()
              .anyMatch(
                  m ->
                      aspectName.equals(m.getAspectName())
                          && (mustContain == null
                              || m.getAspect() != null
                                  && m.getAspect()
                                      .getValue()
                                      .asString(StandardCharsets.UTF_8)
                                      .contains(mustContain)));
      if (!found) {
        missing.add(relative(f));
      }
    }
    assertTrue(
        missing.isEmpty(),
        facet + " did not become " + aspectName + " for:\n" + String.join("\n", missing));
  }

  /**
   * The REST endpoint defaults platformInstance to env, which the Spark agent does not, so the same
   * event produces a different DataFlow cluster on each path. Both are exercised: a config change
   * that silently stopped the cluster following platformInstance would otherwise only show up in a
   * deployment.
   */
  @Test
  public void clusterFollowsPlatformInstanceWhenTheEndpointSetsOne() throws Exception {
    String body = read(new File(corpusRoot(), "airflow/line_02.json"));
    OpenLineage.RunEvent event = OpenLineageClientUtils.runEventFromJson(body);

    DataFlowUrn fromNamespace =
        OpenLineageToDataHub.convertRunEventToJob(event, config()).getFlowUrn();
    DataFlowUrn fromInstance =
        OpenLineageToDataHub.convertRunEventToJob(event, endpointConfig()).getFlowUrn();

    assertEquals(fromNamespace.getClusterEntity(), "airflow", "cluster should be the OL namespace");
    assertEquals(
        fromInstance.getClusterEntity(), "prod", "cluster should be the platform instance");
    // The job identity itself must not move just because the cluster did.
    assertEquals(fromNamespace.getFlowIdEntity(), fromInstance.getFlowIdEntity());
    assertEquals(fromNamespace.getOrchestratorEntity(), fromInstance.getOrchestratorEntity());
  }

  private static DatahubOpenlineageConfig endpointConfig() {
    return DatahubOpenlineageConfig.builder()
        .fabricType(FabricType.PROD)
        .platformInstance("prod")
        .materializeDataset(true)
        .includeSchemaMetadata(true)
        .captureColumnLevelLineage(true)
        .build();
  }

  private static boolean hasDatasetFacet(String body, String facet) throws Exception {
    JsonNode root = MAPPER.readTree(body);
    for (String side : new String[] {"inputs", "outputs"}) {
      for (JsonNode ds : root.path(side)) {
        for (String holder : new String[] {"facets", "inputFacets", "outputFacets"}) {
          if (ds.path(holder).has(facet)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private static List<MetadataChangeProposal> convert(String body) throws Exception {
    OpenLineage.RunEvent event = OpenLineageClientUtils.runEventFromJson(body);
    return OpenLineageToDataHub.convertRunEventToJob(event, config()).toMcps(config());
  }

  private static String read(File f) throws Exception {
    return new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
  }

  private static File corpusRoot() throws Exception {
    return new File(
        CompatibilityCorpusTest.class.getClassLoader().getResource("compatibility").toURI());
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
    return out.stream().sorted(Comparator.comparing(File::getPath)).collect(Collectors.toList());
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

  /**
   * Mirrors how the Spark agent is configured: no platformInstance, so the DataFlow cluster comes
   * from the OpenLineage job namespace. The orchestrator is deliberately unset so producer and
   * facet resolution are exercised rather than short-circuited.
   */
  private static DatahubOpenlineageConfig config() {
    return DatahubOpenlineageConfig.builder()
        .fabricType(FabricType.PROD)
        .materializeDataset(true)
        .includeSchemaMetadata(true)
        .captureColumnLevelLineage(true)
        .build();
  }
}
