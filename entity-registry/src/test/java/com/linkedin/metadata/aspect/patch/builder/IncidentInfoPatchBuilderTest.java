package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.incident.IncidentAssignee;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentStage;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.template.common.GenericPatchTemplate;
import com.linkedin.mxe.MetadataChangeProposal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.testng.annotations.Test;

public class IncidentInfoPatchBuilderTest {

  @Test
  public void testBuildUsesModernGenericJsonPatchEnvelope() throws Exception {
    MetadataChangeProposal proposal =
        new IncidentInfoPatchBuilder()
            .urn(UrnUtils.getUrn("urn:li:incident:test"))
            .setTitle("new title")
            .setStatus(new IncidentStatus().setState(IncidentState.RESOLVED))
            .setPriority(1)
            .setEntities(List.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test)")))
            .build();

    assertEquals(proposal.getChangeType().toString(), "PATCH");
    assertEquals(proposal.getAspect().getContentType(), "application/json-patch+json");

    JsonNode envelope =
        new ObjectMapper()
            .readTree(proposal.getAspect().getValue().asString(StandardCharsets.UTF_8));

    // The inherited default builder emits a bare ops array, which routes through the legacy
    // per-aspect Template engine at apply time. incidentInfo has no registered Template, so
    // that path fails at GMS even though this MCP looks valid to a test that only inspects the
    // serialized operations. The envelope + forceGenericPatch is what routes to the generic
    // patch path instead.
    assertTrue(envelope.isObject());
    assertTrue(envelope.get("forceGenericPatch").asBoolean());
    assertTrue(envelope.get("patch").isArray());
    assertEquals(
        envelope.get("patch").findValuesAsText("path"),
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
            .get("patch")
            .get(0);
    assertEquals(operation.get("op").asText(), "add");
    assertEquals(operation.get("path").asText(), "/assignees");
    assertTrue(operation.get("value").isArray());
    assertEquals(operation.get("value").size(), 0);
  }

  @Test
  public void testClearMethodsEmitRemoveOperationsWithNoValue() throws Exception {
    MetadataChangeProposal proposal =
        new IncidentInfoPatchBuilder()
            .urn(UrnUtils.getUrn("urn:li:incident:test"))
            .clearTitle()
            .clearDescription()
            .clearPriority()
            .build();

    JsonNode ops =
        new ObjectMapper()
            .readTree(proposal.getAspect().getValue().asString(StandardCharsets.UTF_8))
            .get("patch");
    assertEquals(ops.size(), 3);
    for (JsonNode op : ops) {
      assertEquals(op.get("op").asText(), "remove");
      assertFalse(op.has("value"));
    }
    assertEquals(
        ops.findValuesAsText("path"), ImmutableList.of("/title", "/description", "/priority"));
  }

  @Test
  public void testNestedStatusSettersPatchOnlySuppliedSubFields() throws Exception {
    MetadataChangeProposal proposal =
        new IncidentInfoPatchBuilder()
            .urn(UrnUtils.getUrn("urn:li:incident:test"))
            .setStatusState(IncidentState.RESOLVED)
            .setStatusStage(IncidentStage.FIXED)
            .build();

    JsonNode ops =
        new ObjectMapper()
            .readTree(proposal.getAspect().getValue().asString(StandardCharsets.UTF_8))
            .get("patch");
    assertEquals(ops.findValuesAsText("path"), ImmutableList.of("/status/state", "/status/stage"));
    assertEquals(ops.get(0).get("value").asText(), "RESOLVED");
    assertEquals(ops.get(1).get("value").asText(), "FIXED");
  }

  @Test
  public void testStatusLastUpdatedIsPatchedAsANestedSubField() throws Exception {
    AuditStamp lastUpdated =
        new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:datahub")).setTime(42L);

    MetadataChangeProposal proposal =
        new IncidentInfoPatchBuilder()
            .urn(UrnUtils.getUrn("urn:li:incident:test"))
            .setStatusState(IncidentState.RESOLVED)
            .setStatusLastUpdated(lastUpdated)
            .build();

    JsonNode ops =
        new ObjectMapper()
            .readTree(proposal.getAspect().getValue().asString(StandardCharsets.UTF_8))
            .get("patch");
    assertEquals(
        ops.findValuesAsText("path"), ImmutableList.of("/status/state", "/status/lastUpdated"));
    assertEquals(ops.get(1).get("value").get("time").asLong(), 42L);
  }

  @Test
  public void testSetStartedAtEmitsAddOperation() throws Exception {
    MetadataChangeProposal proposal =
        new IncidentInfoPatchBuilder()
            .urn(UrnUtils.getUrn("urn:li:incident:test"))
            .setStartedAt(456L)
            .build();

    JsonNode operation =
        new ObjectMapper()
            .readTree(proposal.getAspect().getValue().asString(StandardCharsets.UTF_8))
            .get("patch")
            .get(0);
    assertEquals(operation.get("op").asText(), "add");
    assertEquals(operation.get("path").asText(), "/startedAt");
    assertEquals(operation.get("value").asLong(), 456L);
  }

  /**
   * Builds the modern generic PATCH MCP this builder now emits and applies it through the same
   * {@link GenericPatchTemplate} runtime path GMS uses for aspects without a registered Template
   * (incidentInfo has none). A test that only inspects serialized operations, as the previous
   * version of this test suite did, would pass even for a bare-array payload that GMS rejects; this
   * exercises the actual apply-time behavior.
   */
  @Test
  public void testGenericPatchAppliesThroughRealPatchPathAndPreservesNonEditorFields()
      throws Exception {
    AuditStamp created =
        new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:datahub")).setTime(0L);
    IncidentInfo existing =
        new IncidentInfo()
            .setType(IncidentType.SQL)
            .setCreated(created)
            .setSource(new IncidentSource().setType(IncidentSourceType.MANUAL))
            .setStartedAt(123L)
            .setTitle("old title")
            .setDescription("old description")
            .setPriority(0)
            .setStatus(new IncidentStatus().setState(IncidentState.ACTIVE).setLastUpdated(created))
            .setEntities(
                new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test)"))))
            .setAssignees(
                new IncidentAssigneeArray(
                    new IncidentAssignee()
                        .setActor(UrnUtils.getUrn("urn:li:corpuser:assignee"))
                        .setAssignedAt(created)));

    MetadataChangeProposal proposal =
        new IncidentInfoPatchBuilder()
            .urn(UrnUtils.getUrn("urn:li:incident:test"))
            .clearDescription()
            .setStatusState(IncidentState.RESOLVED)
            .clearPriority()
            .build();

    GenericJsonPatch genericJsonPatch =
        new ObjectMapper()
            .readValue(
                proposal.getAspect().getValue().asString(StandardCharsets.UTF_8),
                GenericJsonPatch.class);

    GenericPatchTemplate<IncidentInfo> template =
        GenericPatchTemplate.<IncidentInfo>builder()
            .genericJsonPatch(genericJsonPatch)
            .templateType(IncidentInfo.class)
            .templateDefault(new IncidentInfo())
            .build();

    IncidentInfo patched = template.applyPatch(existing);

    // Requested clears took effect.
    assertFalse(patched.hasDescription());
    assertFalse(patched.hasPriority());
    assertEquals(patched.getStatus().getState(), IncidentState.RESOLVED);

    // Non-editor fields, and omitted editor fields, survive untouched.
    assertEquals(patched.getType(), IncidentType.SQL);
    assertEquals(patched.getCreated().getActor(), created.getActor());
    assertEquals(patched.getSource().getType(), IncidentSourceType.MANUAL);
    assertEquals(patched.getStartedAt(), Long.valueOf(123L));
    assertEquals(patched.getTitle(), "old title");
    assertEquals(
        patched.getEntities(),
        new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:dataset:(test,test,test)"))));
    assertEquals(patched.getAssignees().size(), 1);
    assertEquals(
        patched.getAssignees().get(0).getActor(), UrnUtils.getUrn("urn:li:corpuser:assignee"));
  }

  @Test
  public void testStartedAtAndStatusLastUpdatedApplyThroughRealPatchPath() throws Exception {
    AuditStamp created =
        new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:datahub")).setTime(0L);
    IncidentInfo existing =
        new IncidentInfo()
            .setType(IncidentType.SQL)
            .setCreated(created)
            .setStartedAt(1L)
            .setStatus(new IncidentStatus().setState(IncidentState.ACTIVE).setLastUpdated(created));

    AuditStamp newLastUpdated =
        new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:agent")).setTime(999L);

    MetadataChangeProposal proposal =
        new IncidentInfoPatchBuilder()
            .urn(UrnUtils.getUrn("urn:li:incident:test"))
            .setStartedAt(2L)
            .setStatusState(IncidentState.RESOLVED)
            .setStatusLastUpdated(newLastUpdated)
            .build();

    GenericJsonPatch genericJsonPatch =
        new ObjectMapper()
            .readValue(
                proposal.getAspect().getValue().asString(StandardCharsets.UTF_8),
                GenericJsonPatch.class);
    GenericPatchTemplate<IncidentInfo> template =
        GenericPatchTemplate.<IncidentInfo>builder()
            .genericJsonPatch(genericJsonPatch)
            .templateType(IncidentInfo.class)
            .templateDefault(new IncidentInfo())
            .build();

    IncidentInfo patched = template.applyPatch(existing);

    // Backdated startedAt and bumped status.lastUpdated took effect.
    assertEquals(patched.getStartedAt(), Long.valueOf(2L));
    assertEquals(patched.getStatus().getState(), IncidentState.RESOLVED);
    assertEquals(patched.getStatus().getLastUpdated().getTime(), Long.valueOf(999L));
    assertEquals(patched.getStatus().getLastUpdated().getActor(), newLastUpdated.getActor());

    // created is generated at raise time and must never be touched.
    assertEquals(patched.getCreated().getActor(), created.getActor());
  }
}
