package io.datahubproject.openlineage;

import static org.testng.Assert.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.testng.annotations.Test;

/**
 * Reproduces issue #14458: a standard OpenLineage event carrying a {@code TagsJobFacet} and {@code
 * OwnershipJobFacet} on the job (as sent to the REST endpoint) should surface those tags and owners
 * on the resulting DataJob. Previously tags were only read from the Airflow run facet and emitted
 * on the DataFlow, and ownership was computed but never emitted at all.
 */
public class OpenLineageJobFacetTagsOwnershipTest {

  private static final URI PRODUCER =
      URI.create("https://github.com/OpenLineage/OpenLineage/evaluation/openlineage-examples");

  private static DatahubOpenlineageConfig config() {
    return DatahubOpenlineageConfig.builder()
        .fabricType(FabricType.PROD)
        .orchestrator("spark")
        .build();
  }

  private DatahubJob convert() throws Exception {
    OpenLineage ol = new OpenLineage(PRODUCER);

    OpenLineage.TagsJobFacet tagsFacet =
        ol.newTagsJobFacetBuilder()
            .tags(
                Arrays.asList(
                    ol.newTagsJobFacetFieldsBuilder()
                        .key("environment")
                        .value("production")
                        .source("CONFIG")
                        .build(),
                    ol.newTagsJobFacetFieldsBuilder()
                        .key("team")
                        .value("data-engineering")
                        .source("CONFIG")
                        .build()))
            .build();

    OpenLineage.OwnershipJobFacet ownershipFacet =
        ol.newOwnershipJobFacetBuilder()
            .owners(
                Collections.singletonList(
                    ol.newOwnershipJobFacetOwnersBuilder().name("jdoe").type("DEVELOPER").build()))
            .build();

    OpenLineage.RunEvent event =
        ol.newRunEventBuilder()
            .eventTime(ZonedDateTime.now())
            .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
            .run(ol.newRunBuilder().runId(UUID.randomUUID()).build())
            .job(
                ol.newJobBuilder()
                    .namespace("open_lineage_examples")
                    .name("minimal_job")
                    .facets(
                        ol.newJobFacetsBuilder().tags(tagsFacet).ownership(ownershipFacet).build())
                    .build())
            .inputs(Collections.emptyList())
            .outputs(Collections.emptyList())
            .build();

    return OpenLineageToDataHub.convertRunEventToJob(event, config());
  }

  private static String aspectOn(
      List<MetadataChangeProposal> mcps, String entityType, String aspectName) {
    return mcps.stream()
        .filter(m -> entityType.equals(m.getEntityType()) && aspectName.equals(m.getAspectName()))
        .filter(m -> m.getAspect() != null)
        .map(m -> m.getAspect().getValue().asString(StandardCharsets.UTF_8))
        .findFirst()
        .orElse(null);
  }

  @Test
  public void tagsJobFacetSurfacesAsJobGlobalTags() throws Exception {
    List<MetadataChangeProposal> mcps = convert().toMcps(config());
    String globalTags = aspectOn(mcps, "dataJob", "globalTags");
    assertTrue(globalTags != null, "expected a globalTags aspect on the DataJob, got none");
    // key:value naming; colon in the tag name must survive TagUrn construction.
    assertTrue(
        globalTags.contains("environment:production"),
        "DataJob globalTags should contain 'environment:production': " + globalTags);
    assertTrue(
        globalTags.contains("team:data-engineering"),
        "DataJob globalTags should contain 'team:data-engineering': " + globalTags);
  }

  @Test
  public void ownershipJobFacetSurfacesAsJobOwnership() throws Exception {
    List<MetadataChangeProposal> mcps = convert().toMcps(config());
    String ownership = aspectOn(mcps, "dataJob", "ownership");
    assertTrue(ownership != null, "expected an ownership aspect on the DataJob, got none");
    assertTrue(
        ownership.contains("urn:li:corpuser:jdoe"),
        "DataJob ownership should contain 'urn:li:corpuser:jdoe': " + ownership);
  }
}
