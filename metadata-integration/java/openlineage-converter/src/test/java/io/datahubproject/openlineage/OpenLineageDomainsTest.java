package io.datahubproject.openlineage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class OpenLineageDomainsTest {

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

  private static Map<String, List<String>> domainUrnsByEntity(List<String> configuredDomains)
      throws Exception {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .orchestrator("spark")
            .domains(configuredDomains)
            .build();
    DatahubJob job = OpenLineageToDataHub.convertRunEventToJob(runEvent(), config);
    return job.toMcps(config).stream()
        .filter(mcp -> "domains".equals(mcp.getAspectName()))
        .collect(
            Collectors.groupingBy(
                MetadataChangeProposal::getEntityType,
                Collectors.mapping(mcp -> mcp.getEntityUrn().toString(), Collectors.toList())));
  }

  @Test
  public void testConfiguredDomainsLandOnFlowAndJob() throws Exception {
    Map<String, List<String>> byEntity = domainUrnsByEntity(List.of("urn:li:domain:finance"));

    assertEquals(byEntity.keySet().size(), 2, "expected a domains aspect on both flow and job");
    assertTrue(
        byEntity.containsKey(DatahubJob.DATA_FLOW_ENTITY_TYPE),
        "missing DataFlow domains aspect: " + byEntity);
    assertTrue(
        byEntity.containsKey(DatahubJob.DATAJOB_ENTITY_TYPE),
        "missing DataJob domains aspect: " + byEntity);
  }

  @Test
  public void testNoDomainsConfiguredEmitsNoDomainsAspect() throws Exception {
    assertTrue(domainUrnsByEntity(List.of()).isEmpty());
  }

  /**
   * A domain name is not resolvable to a URN here, so nothing is emitted rather than an empty
   * aspect that would clear domains set elsewhere.
   */
  @Test
  public void testUnparseableDomainEmitsNothing() throws Exception {
    assertTrue(domainUrnsByEntity(List.of("finance")).isEmpty());
  }

  /** A syntactically valid URN for some other entity is not a domain and must not be emitted. */
  @Test
  public void testNonDomainUrnIsRejected() throws Exception {
    assertTrue(domainUrnsByEntity(List.of("urn:li:corpuser:jdoe")).isEmpty());
  }

  @Test
  public void testValidDomainsSurviveAlongsideRejectedOnes() throws Exception {
    Map<String, List<String>> byEntity =
        domainUrnsByEntity(List.of("urn:li:corpuser:jdoe", "urn:li:domain:finance", "finance"));
    assertEquals(byEntity.keySet().size(), 2);
  }

  @Test
  public void testImmutableConfiguredDomainListIsNotMutated() {
    List<String> immutable = List.of("urn:li:domain:zeta", "urn:li:domain:alpha");
    OpenLineageToDataHub.generateDomains(immutable);
    assertEquals(immutable, List.of("urn:li:domain:zeta", "urn:li:domain:alpha"));
  }
}
