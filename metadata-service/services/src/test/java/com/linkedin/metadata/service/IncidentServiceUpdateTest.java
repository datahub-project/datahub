package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IncidentServiceUpdateTest {

  private static final Urn INCIDENT_URN = UrnUtils.getUrn("urn:li:incident:test");

  @Test
  public void patchProposalContainsOnlySuppliedFields() throws Exception {
    SystemEntityClient client = mock(SystemEntityClient.class);
    IncidentService service = new IncidentService(client);

    service.updateIncident(
        mock(OperationContext.class),
        INCIDENT_URN,
        IncidentInfoPatch.builder().title("new title").priority(1).build());

    MetadataChangeProposal proposal = captureProposal(client);
    Assert.assertEquals(proposal.getChangeType(), ChangeType.PATCH);
    Assert.assertEquals(proposal.getAspect().getContentType(), "application/json-patch+json");
    List<String> paths = patchOpPaths(proposal);
    Assert.assertEquals(paths, List.of("/title", "/priority"));
    Mockito.verify(client, Mockito.never()).getV2(any(), any(), any(), any());
  }

  @Test
  public void patchProposalUsesModernGenericPatchEnvelope() throws Exception {
    SystemEntityClient client = mock(SystemEntityClient.class);
    IncidentService service = new IncidentService(client);

    service.updateIncident(
        mock(OperationContext.class),
        INCIDENT_URN,
        IncidentInfoPatch.builder().title("new title").build());

    JsonNode envelope = envelopeNode(captureProposal(client));
    Assert.assertTrue(envelope.isObject());
    Assert.assertTrue(envelope.get("forceGenericPatch").asBoolean());
    Assert.assertTrue(envelope.get("patch").isArray());
  }

  @Test
  public void nestedStatusUpdatePatchesOnlySuppliedSubFieldsPlusLastUpdated() throws Exception {
    SystemEntityClient client = mock(SystemEntityClient.class);
    IncidentService service = new IncidentService(client);
    AuditStamp lastUpdated =
        new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:agent")).setTime(42L);

    service.updateIncident(
        mock(OperationContext.class),
        INCIDENT_URN,
        IncidentInfoPatch.builder()
            .status(
                new IncidentStatus().setState(IncidentState.RESOLVED).setLastUpdated(lastUpdated))
            .build());

    List<String> paths = patchOpPaths(captureProposal(client));
    Assert.assertEquals(paths, List.of("/status/state", "/status/lastUpdated"));
  }

  @Test
  public void nestedStatusUpdateWithoutLastUpdatedDoesNotThrowOrPatchIt() throws Exception {
    SystemEntityClient client = mock(SystemEntityClient.class);
    IncidentService service = new IncidentService(client);

    // A caller building IncidentInfoPatch directly (bypassing IncidentUtils.mapIncidentStatus,
    // which always stamps lastUpdated) may supply a status without it set. lastUpdated is a
    // required field on IncidentStatus, so calling getLastUpdated() unconditionally would throw.
    service.updateIncident(
        mock(OperationContext.class),
        INCIDENT_URN,
        IncidentInfoPatch.builder()
            .status(new IncidentStatus().setState(IncidentState.RESOLVED))
            .build());

    List<String> paths = patchOpPaths(captureProposal(client));
    Assert.assertEquals(paths, List.of("/status/state"));
  }

  @Test
  public void updateIncidentPatchesStartedAt() throws Exception {
    SystemEntityClient client = mock(SystemEntityClient.class);
    IncidentService service = new IncidentService(client);

    service.updateIncident(
        mock(OperationContext.class),
        INCIDENT_URN,
        IncidentInfoPatch.builder().startedAt(10L).build());

    Map<String, JsonNode> opsByPath = patchOpsByPath(captureProposal(client));
    Assert.assertEquals(opsByPath.get("/startedAt").get("op").asText(), "add");
    Assert.assertEquals(opsByPath.get("/startedAt").get("value").asLong(), 10L);
  }

  @Test
  public void upsertProducesPatchOnlyWriteWithExplicitClears() throws Exception {
    SystemEntityClient client = mock(SystemEntityClient.class);
    IncidentService service = new IncidentService(client);

    service.upsertIncident(
        mock(OperationContext.class),
        INCIDENT_URN,
        IncidentInfoUpsert.builder()
            .title("new title")
            .description(null)
            .status(new IncidentStatus().setState(IncidentState.RESOLVED))
            .priority(null)
            .entities(List.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test2)")))
            .assignees(new IncidentAssigneeArray())
            .build());

    MetadataChangeProposal proposal = captureProposal(client);
    Assert.assertEquals(proposal.getChangeType(), ChangeType.PATCH);
    Assert.assertEquals(proposal.getAspect().getContentType(), "application/json-patch+json");
    Mockito.verify(client, Mockito.never()).getV2(any(), any(), any(), any());

    Map<String, JsonNode> opsByPath = patchOpsByPath(proposal);
    Assert.assertEquals(opsByPath.get("/title").get("op").asText(), "add");
    Assert.assertEquals(opsByPath.get("/title").get("value").asText(), "new title");
    Assert.assertEquals(opsByPath.get("/description").get("op").asText(), "remove");
    Assert.assertEquals(opsByPath.get("/priority").get("op").asText(), "remove");
    Assert.assertEquals(opsByPath.get("/status").get("op").asText(), "add");
    Assert.assertEquals(opsByPath.get("/entities").get("op").asText(), "add");
    Assert.assertEquals(opsByPath.get("/assignees").get("op").asText(), "add");
    Assert.assertTrue(opsByPath.get("/assignees").get("value").isArray());
    Assert.assertEquals(opsByPath.get("/assignees").get("value").size(), 0);
    // startedAt is not editor-owned; upsertIncident must never target it.
    Assert.assertFalse(opsByPath.containsKey("/startedAt"));
  }

  @Test
  public void upsertWithNullAssigneesClearsAssignees() throws Exception {
    SystemEntityClient client = mock(SystemEntityClient.class);
    IncidentService service = new IncidentService(client);

    service.upsertIncident(
        mock(OperationContext.class),
        INCIDENT_URN,
        IncidentInfoUpsert.builder()
            .title("t")
            .status(new IncidentStatus().setState(IncidentState.ACTIVE))
            .entities(List.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test)")))
            .assignees(null)
            .build());

    Map<String, JsonNode> opsByPath = patchOpsByPath(captureProposal(client));
    Assert.assertEquals(opsByPath.get("/assignees").get("op").asText(), "add");
    Assert.assertTrue(opsByPath.get("/assignees").get("value").isArray());
    Assert.assertEquals(opsByPath.get("/assignees").get("value").size(), 0);
  }

  @Test
  public void upsertRequiresStatusAndNonEmptyEntities() {
    SystemEntityClient client = mock(SystemEntityClient.class);
    IncidentService service = new IncidentService(client);

    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            service.upsertIncident(
                mock(OperationContext.class),
                INCIDENT_URN,
                IncidentInfoUpsert.builder()
                    .entities(List.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test)")))
                    .build()));

    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            service.upsertIncident(
                mock(OperationContext.class),
                INCIDENT_URN,
                IncidentInfoUpsert.builder()
                    .status(new IncidentStatus().setState(IncidentState.ACTIVE))
                    .entities(List.of())
                    .build()));
  }

  private static MetadataChangeProposal captureProposal(SystemEntityClient client)
      throws Exception {
    ArgumentCaptor<MetadataChangeProposal> captor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(client)
        .ingestProposal(any(OperationContext.class), captor.capture(), Mockito.eq(false));
    return captor.getValue();
  }

  private static JsonNode envelopeNode(MetadataChangeProposal proposal) throws Exception {
    return new ObjectMapper()
        .readTree(proposal.getAspect().getValue().asString(StandardCharsets.UTF_8));
  }

  private static List<String> patchOpPaths(MetadataChangeProposal proposal) throws Exception {
    return envelopeNode(proposal).get("patch").findValuesAsText("path");
  }

  private static Map<String, JsonNode> patchOpsByPath(MetadataChangeProposal proposal)
      throws Exception {
    Map<String, JsonNode> byPath = new HashMap<>();
    for (JsonNode op : envelopeNode(proposal).get("patch")) {
      byPath.put(op.get("path").asText(), op);
    }
    return byPath;
  }
}
