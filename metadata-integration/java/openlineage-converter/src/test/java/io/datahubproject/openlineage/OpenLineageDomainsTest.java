package io.datahubproject.openlineage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.FabricType;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.testng.annotations.Test;

public class OpenLineageDomainsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static OpenLineage.RunEvent runEvent() {
    OpenLineage ol = new OpenLineage(URI.create("https://github.com/OpenLineage/OpenLineage"));
    return ol.newRunEventBuilder()
        .eventTime(ZonedDateTime.now())
        .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
        .run(ol.newRunBuilder().runId(UUID.randomUUID()).build())
        .job(ol.newJobBuilder().namespace("my_namespace").name("my_job").build())
        .inputs(java.util.Collections.emptyList())
        .outputs(java.util.Collections.emptyList())
        .build();
  }

  /** Maps entity type to the domain URNs actually carried in that entity's domains aspect. */
  private static Map<String, List<String>> emittedDomains(List<String> configuredDomains)
      throws Exception {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .orchestrator("spark")
            .domains(configuredDomains)
            .build();
    DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(runEvent(), config);

    Map<String, List<String>> byEntity = new HashMap<>();
    for (MetadataChangeProposal mcp : job.toMcps(config)) {
      if (!"domains".equals(mcp.getAspectName())) {
        continue;
      }
      JsonNode aspect =
          MAPPER.readTree(mcp.getAspect().getValue().asString(StandardCharsets.UTF_8));
      List<String> urns = new ArrayList<>();
      aspect.get("domains").forEach(n -> urns.add(n.asText()));
      byEntity.put(mcp.getEntityType(), urns);
    }
    return byEntity;
  }

  @Test
  public void testConfiguredDomainsLandOnFlowAndJob() throws Exception {
    Map<String, List<String>> byEntity =
        emittedDomains(List.of("urn:li:domain:reporting", "urn:li:domain:finance"));

    List<String> expected = List.of("urn:li:domain:finance", "urn:li:domain:reporting");
    assertEquals(byEntity.get(DatahubJob.DATA_FLOW_ENTITY_TYPE), expected);
    assertEquals(byEntity.get(DatahubJob.DATAJOB_ENTITY_TYPE), expected);
    assertEquals(byEntity.size(), 2, "domains should reach the flow and the job only: " + byEntity);
  }

  @Test
  public void testNoDomainsConfiguredEmitsNoDomainsAspect() throws Exception {
    assertTrue(emittedDomains(List.of()).isEmpty());
  }

  /**
   * A domain name is not resolvable to a URN here, so nothing is emitted rather than an empty
   * aspect that would clear domains set elsewhere.
   */
  @Test
  public void testUnparseableDomainEmitsNothing() throws Exception {
    assertTrue(emittedDomains(List.of("finance")).isEmpty());
  }

  /** A syntactically valid URN for some other entity is not a domain and must not be emitted. */
  @Test
  public void testNonDomainUrnIsRejected() throws Exception {
    assertTrue(emittedDomains(List.of("urn:li:corpuser:jdoe")).isEmpty());
  }

  /** "urn:li:domain" parses as entity type "domain" but carries no id. */
  @Test
  public void testDomainUrnWithoutIdIsRejected() throws Exception {
    assertTrue(emittedDomains(List.of("urn:li:domain")).isEmpty());
  }

  @Test
  public void testOnlyValidDomainsSurviveAlongsideRejectedOnes() throws Exception {
    Map<String, List<String>> byEntity =
        emittedDomains(
            List.of("urn:li:corpuser:jdoe", "urn:li:domain:finance", "finance", "urn:li:domain"));

    List<String> expected = List.of("urn:li:domain:finance");
    assertEquals(byEntity.get(DatahubJob.DATA_FLOW_ENTITY_TYPE), expected);
    assertEquals(byEntity.get(DatahubJob.DATAJOB_ENTITY_TYPE), expected);
  }

  @Test
  public void testImmutableConfiguredDomainListIsNotMutated() {
    List<String> immutable = List.of("urn:li:domain:zeta", "urn:li:domain:alpha");
    OpenLineageToDataHub.generateDomains(immutable);
    assertEquals(immutable, List.of("urn:li:domain:zeta", "urn:li:domain:alpha"));
  }
}
