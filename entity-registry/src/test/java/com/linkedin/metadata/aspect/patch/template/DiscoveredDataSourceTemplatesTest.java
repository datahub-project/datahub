package com.linkedin.metadata.aspect.patch.template;

import static com.linkedin.metadata.Constants.DISCOVERED_DATA_SOURCE_DECISION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DISCOVERED_DATA_SOURCE_EVIDENCE_ASPECT_NAME;

import com.linkedin.ingestion.discovery.DiscoveredDataSourceDecision;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceEvidence;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceEvidenceEntry;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceEvidenceEntryMap;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceStatus;
import com.linkedin.ingestion.discovery.DiscoveredDataSourceTier;
import com.linkedin.metadata.aspect.patch.template.ingestion.DiscoveredDataSourceDecisionTemplate;
import com.linkedin.metadata.aspect.patch.template.ingestion.DiscoveredDataSourceEvidenceTemplate;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DiscoveredDataSourceTemplatesTest {

  private static final String TABLEAU_KEY = "tableau.datasource:urn:li:dataHubIngestionSource:t1";
  private static final String FIVETRAN_KEY = "fivetran.connector:urn:li:dataHubIngestionSource:f1";

  @Test
  public void testTemplatesAreRegistered() {
    AspectTemplateEngine engine = SnapshotEntityRegistry.getInstance().getAspectTemplateEngine();
    Assert.assertNotNull(engine.getDefaultTemplate(DISCOVERED_DATA_SOURCE_EVIDENCE_ASPECT_NAME));
    Assert.assertNotNull(engine.getDefaultTemplate(DISCOVERED_DATA_SOURCE_DECISION_ASPECT_NAME));
  }

  /** Two connectors write the same platform; each patch must only touch its own entry. */
  @Test
  public void testEvidencePatchReplacesOnlyItsOwnEntry() throws Exception {
    DiscoveredDataSourceEvidenceTemplate template = new DiscoveredDataSourceEvidenceTemplate();
    DiscoveredDataSourceEvidence existing =
        new DiscoveredDataSourceEvidence()
            .setEvidence(
                new DiscoveredDataSourceEvidenceEntryMap(
                    Map.of(FIVETRAN_KEY, entry("fivetran.connector", 1L))));

    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/evidence/" + TABLEAU_KEY)
                        .add(
                            "value",
                            Json.createObjectBuilder()
                                .add("detector", "tableau.datasource")
                                .add("tier", "CONFIGURED_CONNECTION")
                                .add("lastSeen", 2L)))
                .build());

    DiscoveredDataSourceEvidence result = template.applyPatch(existing, patch);

    Assert.assertEquals(result.getEvidence().size(), 2);
    Assert.assertEquals(result.getEvidence().get(FIVETRAN_KEY).getLastSeen(), 1L);
    Assert.assertEquals(
        result.getEvidence().get(TABLEAU_KEY).getTier(),
        DiscoveredDataSourceTier.CONFIGURED_CONNECTION);
  }

  @Test
  public void testDecisionPatchSetsStatusAndAppendsHistory() throws Exception {
    DiscoveredDataSourceDecisionTemplate template = new DiscoveredDataSourceDecisionTemplate();

    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/status")
                        .add("value", "PRESENTED"))
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/history/-")
                        .add(
                            "value",
                            Json.createObjectBuilder()
                                .add("status", "PRESENTED")
                                .add("actor", "urn:li:corpuser:datahub")
                                .add("time", 3L)))
                .build());

    DiscoveredDataSourceDecision result = template.applyPatch(template.getDefault(), patch);

    Assert.assertEquals(result.getStatus(), DiscoveredDataSourceStatus.PRESENTED);
    Assert.assertEquals(result.getHistory().size(), 1);
  }

  private static DiscoveredDataSourceEvidenceEntry entry(String detector, long lastSeen) {
    return new DiscoveredDataSourceEvidenceEntry()
        .setDetector(detector)
        .setTier(DiscoveredDataSourceTier.FINGERPRINT)
        .setLastSeen(lastSeen);
  }
}
