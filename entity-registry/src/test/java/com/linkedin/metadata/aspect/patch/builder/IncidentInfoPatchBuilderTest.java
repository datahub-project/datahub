package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.mxe.MetadataChangeProposal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.testng.annotations.Test;

public class IncidentInfoPatchBuilderTest {

  @Test
  public void testBuildContainsOnlySuppliedFields() throws Exception {
    MetadataChangeProposal proposal =
        new IncidentInfoPatchBuilder()
            .urn(UrnUtils.getUrn("urn:li:incident:test"))
            .setTitle("new title")
            .setStatus(new IncidentStatus().setState(IncidentState.RESOLVED))
            .setPriority(1)
            .setEntities(List.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test)")))
            .build();

    JsonNode operations =
        new ObjectMapper()
            .readTree(proposal.getAspect().getValue().asString(StandardCharsets.UTF_8));

    assertEquals(proposal.getChangeType().toString(), "PATCH");
    assertEquals(proposal.getAspect().getContentType(), "application/json-patch+json");
    assertTrue(operations.isArray());
    assertEquals(
        operations.findValuesAsText("path"),
        ImmutableList.of("/title", "/status", "/priority", "/entities"));
  }

  @Test
  public void testEmptyAssigneeArrayIsAnExplicitClear() throws Exception {
    MetadataChangeProposal proposal =
        new IncidentInfoPatchBuilder()
            .urn(UrnUtils.getUrn("urn:li:incident:test"))
            .setAssignees(new IncidentAssigneeArray())
            .build();

    JsonNode operation =
        new ObjectMapper()
            .readTree(proposal.getAspect().getValue().asString(StandardCharsets.UTF_8))
            .get(0);
    assertEquals(operation.get("path").asText(), "/assignees");
    assertTrue(operation.get("value").isArray());
    assertEquals(operation.get("value").size(), 0);
  }
}
