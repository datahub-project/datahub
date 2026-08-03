package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.nio.charset.StandardCharsets;
import java.util.List;
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
        IncidentInfoUpdate.builder().title("new title").priority(1).build());

    MetadataChangeProposal proposal = captureProposal(client);
    Assert.assertEquals(proposal.getChangeType(), ChangeType.PATCH);
    Assert.assertEquals(proposal.getAspect().getContentType(), "application/json-patch+json");
    JsonNode operations =
        new ObjectMapper()
            .readTree(proposal.getAspect().getValue().asString(StandardCharsets.UTF_8));
    Assert.assertEquals(operations.findValuesAsText("path"), List.of("/title", "/priority"));
    Mockito.verify(client, Mockito.never()).getV2(any(), any(), any(), any());
  }

  @Test
  public void replacementClearsNullableEditorFieldsAndPreservesMetadata() throws Exception {
    SystemEntityClient client = mock(SystemEntityClient.class);
    IncidentService service = new IncidentService(client);
    IncidentInfo existing =
        new IncidentInfo()
            .setType(IncidentType.SQL)
            .setTitle("old title")
            .setDescription("old description")
            .setPriority(0)
            .setStartedAt(5L)
            .setSource(new IncidentSource().setType(IncidentSourceType.MANUAL))
            .setEntities(
                new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test)"))))
            .setAssignees(new IncidentAssigneeArray())
            .setStatus(new IncidentStatus().setState(IncidentState.ACTIVE));

    service.replaceIncident(
        mock(OperationContext.class),
        INCIDENT_URN,
        existing,
        IncidentInfoUpdate.builder()
            .title("new title")
            .description(null)
            .status(new IncidentStatus().setState(IncidentState.RESOLVED))
            .priority(null)
            .entities(List.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test2)")))
            .assignees(new IncidentAssigneeArray())
            .build());

    IncidentInfo updated =
        GenericRecordUtils.deserializeAspect(
            captureProposal(client).getAspect().getValue(), "application/json", IncidentInfo.class);
    Assert.assertEquals(updated.getTitle(), "new title");
    Assert.assertFalse(updated.hasDescription());
    Assert.assertFalse(updated.hasPriority());
    Assert.assertEquals(updated.getStartedAt(), Long.valueOf(5L));
    Assert.assertEquals(updated.getType(), IncidentType.SQL);
    Assert.assertEquals(updated.getSource().getType(), IncidentSourceType.MANUAL);
    Assert.assertEquals(
        updated.getEntities(),
        new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test2)"))));
    Assert.assertTrue(updated.hasAssignees());
    Assert.assertTrue(updated.getAssignees().isEmpty());
    Assert.assertEquals(updated.getStatus().getState(), IncidentState.RESOLVED);
  }

  private static MetadataChangeProposal captureProposal(SystemEntityClient client)
      throws Exception {
    ArgumentCaptor<MetadataChangeProposal> captor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(client)
        .ingestProposal(any(OperationContext.class), captor.capture(), Mockito.eq(false));
    return captor.getValue();
  }
}
