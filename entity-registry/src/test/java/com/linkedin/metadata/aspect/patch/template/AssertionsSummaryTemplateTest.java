package com.linkedin.metadata.aspect.patch.template;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AssertionSummaryDetails;
import com.linkedin.common.AssertionSummaryDetailsArray;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.template.assertion.AssertionsSummaryTemplate;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonPatchBuilder;
import org.testng.annotations.Test;

public class AssertionsSummaryTemplateTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_ASSERTION_URN_2 = UrnUtils.getUrn("urn:li:assertion:test2");

  @Test
  public void testAddPassingDetails() throws Exception {
    AssertionsSummaryTemplate assertionsSummaryTemplate = new AssertionsSummaryTemplate();
    AssertionsSummary baseSummary = assertionsSummaryTemplate.getDefault();

    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();
    JsonObjectBuilder detailsNode = Json.createObjectBuilder();

    detailsNode.add("urn", TEST_ASSERTION_URN.toString());
    detailsNode.add("type", "FRESHNESS");
    detailsNode.add("lastResultAt", 1234567890L);
    detailsNode.add("source", "NATIVE");

    jsonPatchBuilder.add("/passingAssertionDetails/urn:li:assertion:test", detailsNode.build());

    // Case 1: Initial population test
    AssertionsSummary result =
        assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    assertEquals(result.getPassingAssertionDetails().size(), 1);
    assertEquals(result.getPassingAssertionDetails().get(0).getUrn(), TEST_ASSERTION_URN);
    assertEquals(result.getPassingAssertionDetails().get(0).getType(), "FRESHNESS");
    assertEquals(result.getPassingAssertionDetails().get(0).getLastResultAt(), 1234567890L);
    assertEquals(result.getPassingAssertionDetails().get(0).getSource(), "NATIVE");

    // Case 2: Test non-overwrite existing assertions using new assertion URN
    jsonPatchBuilder = Json.createPatchBuilder();
    detailsNode = Json.createObjectBuilder();

    detailsNode.add("urn", TEST_ASSERTION_URN_2.toString());
    detailsNode.add("type", "FRESHNESS");
    detailsNode.add("lastResultAt", 1234567890L);
    detailsNode.add("source", "NATIVE");

    jsonPatchBuilder.add("/passingAssertionDetails/urn:li:assertion:test2", detailsNode.build());

    result = assertionsSummaryTemplate.applyPatch(result, jsonPatchBuilder.build());

    assertEquals(result.getPassingAssertionDetails().size(), 2);
    assertEquals(result.getPassingAssertionDetails().get(0).getUrn(), TEST_ASSERTION_URN);
    assertEquals(result.getPassingAssertionDetails().get(0).getType(), "FRESHNESS");
    assertEquals(result.getPassingAssertionDetails().get(0).getLastResultAt(), 1234567890L);
    assertEquals(result.getPassingAssertionDetails().get(0).getSource(), "NATIVE");
    assertEquals(result.getPassingAssertionDetails().get(1).getUrn(), TEST_ASSERTION_URN_2);
    assertEquals(result.getPassingAssertionDetails().get(1).getType(), "FRESHNESS");
    assertEquals(result.getPassingAssertionDetails().get(1).getLastResultAt(), 1234567890L);
    assertEquals(result.getPassingAssertionDetails().get(1).getSource(), "NATIVE");
  }

  @Test
  public void testRemovePassingDetails() throws Exception {
    AssertionsSummaryTemplate assertionsSummaryTemplate = new AssertionsSummaryTemplate();
    AssertionsSummary baseSummary = assertionsSummaryTemplate.getDefault();

    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();

    jsonPatchBuilder.remove("/passingAssertionDetails/urn:li:assertion:test");

    // Case 1: Initial removal test - no changes
    AssertionsSummary result =
        assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    assertEquals(result.getPassingAssertionDetails().size(), 0);

    // Case 2: Remove existing assertion
    jsonPatchBuilder = Json.createPatchBuilder();

    baseSummary = new AssertionsSummary();
    baseSummary.setPassingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(TEST_ASSERTION_URN)
                    .setType("FRESHNESS")
                    .setLastResultAt(1234567890L)
                    .setSource("NATIVE"))));

    jsonPatchBuilder.remove("/passingAssertionDetails/urn:li:assertion:test");

    result = assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    // Validate that we successfully removed.
    assertEquals(result.getPassingAssertionDetails().size(), 0);

    // Case 2: Remove existing assertion, but don't touch unrelated one.
    jsonPatchBuilder = Json.createPatchBuilder();

    baseSummary = new AssertionsSummary();
    baseSummary.setPassingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(TEST_ASSERTION_URN)
                    .setType("FRESHNESS")
                    .setLastResultAt(1234567890L)
                    .setSource("NATIVE"),
                new AssertionSummaryDetails()
                    .setUrn(TEST_ASSERTION_URN_2)
                    .setType("VOLUME")
                    .setLastResultAt(1234567890L)
                    .setSource("NATIVE"))));

    jsonPatchBuilder.remove("/passingAssertionDetails/urn:li:assertion:test");

    result = assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    // Validate that we successfully removed, but did not disturb the other assertion.
    assertEquals(result.getPassingAssertionDetails().size(), 1);
    assertEquals(result.getPassingAssertionDetails().get(0).getUrn(), TEST_ASSERTION_URN_2);
    assertEquals(result.getPassingAssertionDetails().get(0).getType(), "VOLUME");
    assertEquals(result.getPassingAssertionDetails().get(0).getLastResultAt(), 1234567890L);
    assertEquals(result.getPassingAssertionDetails().get(0).getSource(), "NATIVE");
  }

  @Test
  public void testAddFailingDetails() throws Exception {
    AssertionsSummaryTemplate assertionsSummaryTemplate = new AssertionsSummaryTemplate();
    AssertionsSummary baseSummary = assertionsSummaryTemplate.getDefault();

    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();
    JsonObjectBuilder detailsNode = Json.createObjectBuilder();

    detailsNode.add("urn", TEST_ASSERTION_URN.toString());
    detailsNode.add("type", "FRESHNESS");
    detailsNode.add("lastResultAt", 1234567890L);
    detailsNode.add("source", "NATIVE");

    jsonPatchBuilder.add("/failingAssertionDetails/urn:li:assertion:test", detailsNode.build());

    // Case 1: Initial population test
    AssertionsSummary result =
        assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    assertEquals(result.getFailingAssertionDetails().size(), 1);
    assertEquals(result.getFailingAssertionDetails().get(0).getUrn(), TEST_ASSERTION_URN);
    assertEquals(result.getFailingAssertionDetails().get(0).getType(), "FRESHNESS");
    assertEquals(result.getFailingAssertionDetails().get(0).getLastResultAt(), 1234567890L);
    assertEquals(result.getFailingAssertionDetails().get(0).getSource(), "NATIVE");

    // Case 2: Test non-overwrite existing assertions using new assertion URN
    jsonPatchBuilder = Json.createPatchBuilder();
    detailsNode = Json.createObjectBuilder();

    detailsNode.add("urn", TEST_ASSERTION_URN_2.toString());
    detailsNode.add("type", "FRESHNESS");
    detailsNode.add("lastResultAt", 1234567890L);
    detailsNode.add("source", "NATIVE");

    jsonPatchBuilder.add("/failingAssertionDetails/urn:li:assertion:test2", detailsNode.build());

    result = assertionsSummaryTemplate.applyPatch(result, jsonPatchBuilder.build());

    assertEquals(result.getFailingAssertionDetails().size(), 2);
    assertEquals(result.getFailingAssertionDetails().get(0).getUrn(), TEST_ASSERTION_URN);
    assertEquals(result.getFailingAssertionDetails().get(0).getType(), "FRESHNESS");
    assertEquals(result.getFailingAssertionDetails().get(0).getLastResultAt(), 1234567890L);
    assertEquals(result.getFailingAssertionDetails().get(0).getSource(), "NATIVE");
    assertEquals(result.getFailingAssertionDetails().get(1).getUrn(), TEST_ASSERTION_URN_2);
    assertEquals(result.getFailingAssertionDetails().get(1).getType(), "FRESHNESS");
    assertEquals(result.getFailingAssertionDetails().get(1).getLastResultAt(), 1234567890L);
    assertEquals(result.getFailingAssertionDetails().get(1).getSource(), "NATIVE");
  }

  @Test
  public void testRemoveFailingDetails() throws Exception {
    AssertionsSummaryTemplate assertionsSummaryTemplate = new AssertionsSummaryTemplate();
    AssertionsSummary baseSummary = assertionsSummaryTemplate.getDefault();

    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();

    jsonPatchBuilder.remove("/failingAssertionDetails/urn:li:assertion:test");

    // Case 1: Initial removal test - no changes
    AssertionsSummary result =
        assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    assertEquals(result.getFailingAssertionDetails().size(), 0);

    // Case 2: Remove existing assertion
    jsonPatchBuilder = Json.createPatchBuilder();

    baseSummary = new AssertionsSummary();
    baseSummary.setFailingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(TEST_ASSERTION_URN)
                    .setType("FRESHNESS")
                    .setLastResultAt(1234567890L)
                    .setSource("NATIVE"))));

    jsonPatchBuilder.remove("/failingAssertionDetails/urn:li:assertion:test");

    result = assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    // Validate that we successfully removed.
    assertEquals(result.getFailingAssertionDetails().size(), 0);

    // Case 2: Remove existing assertion, but don't touch unrelated one.
    jsonPatchBuilder = Json.createPatchBuilder();

    baseSummary = new AssertionsSummary();
    baseSummary.setFailingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(TEST_ASSERTION_URN)
                    .setType("FRESHNESS")
                    .setLastResultAt(1234567890L)
                    .setSource("NATIVE"),
                new AssertionSummaryDetails()
                    .setUrn(TEST_ASSERTION_URN_2)
                    .setType("VOLUME")
                    .setLastResultAt(1234567890L)
                    .setSource("NATIVE"))));

    jsonPatchBuilder.remove("/failingAssertionDetails/urn:li:assertion:test");

    result = assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    // Validate that we successfully removed, but did not disturb the other assertion.
    assertEquals(result.getFailingAssertionDetails().size(), 1);
    assertEquals(result.getFailingAssertionDetails().get(0).getUrn(), TEST_ASSERTION_URN_2);
    assertEquals(result.getFailingAssertionDetails().get(0).getType(), "VOLUME");
    assertEquals(result.getFailingAssertionDetails().get(0).getLastResultAt(), 1234567890L);
    assertEquals(result.getFailingAssertionDetails().get(0).getSource(), "NATIVE");
  }

  @Test
  public void testAddLegacyPassingUrns() throws Exception {
    AssertionsSummaryTemplate assertionsSummaryTemplate = new AssertionsSummaryTemplate();
    AssertionsSummary baseSummary = assertionsSummaryTemplate.getDefault();

    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();

    jsonPatchBuilder.add("/passingAssertions/urn:li:assertion:test", TEST_ASSERTION_URN.toString());

    // Case 1: Initial population test
    AssertionsSummary result =
        assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    assertEquals(result.getPassingAssertions().size(), 1);
    assertEquals(result.getPassingAssertions().get(0), TEST_ASSERTION_URN);

    // Case 2: Test non-overwrite existing assertions using new assertion URN
    jsonPatchBuilder = Json.createPatchBuilder();

    jsonPatchBuilder.add(
        "/passingAssertions/urn:li:assertion:test2", TEST_ASSERTION_URN_2.toString());

    result = assertionsSummaryTemplate.applyPatch(result, jsonPatchBuilder.build());

    assertEquals(result.getPassingAssertions().size(), 2);
    assertEquals(result.getPassingAssertions().get(0), TEST_ASSERTION_URN);
    assertEquals(result.getPassingAssertions().get(1), TEST_ASSERTION_URN_2);
  }

  @Test
  public void testRemoveLegacyPassingUrns() throws Exception {
    AssertionsSummaryTemplate assertionsSummaryTemplate = new AssertionsSummaryTemplate();
    AssertionsSummary baseSummary = assertionsSummaryTemplate.getDefault();

    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();

    jsonPatchBuilder.remove("/passingAssertions/urn:li:assertion:test");

    // Case 1: Initial removal test - no changes
    AssertionsSummary result =
        assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    assertEquals(result.getPassingAssertions().size(), 0);

    // Case 2: Remove existing assertion
    jsonPatchBuilder = Json.createPatchBuilder();

    baseSummary = new AssertionsSummary();
    baseSummary.setPassingAssertions(new UrnArray(ImmutableList.of(TEST_ASSERTION_URN)));

    jsonPatchBuilder.remove("/passingAssertions/urn:li:assertion:test");

    result = assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    // Validate that we successfully removed.
    assertEquals(result.getPassingAssertions().size(), 0);

    // Case 2: Remove existing assertion, but don't touch unrelated one.
    jsonPatchBuilder = Json.createPatchBuilder();

    baseSummary = new AssertionsSummary();
    baseSummary.setPassingAssertions(
        new UrnArray(ImmutableList.of(TEST_ASSERTION_URN, TEST_ASSERTION_URN_2)));

    jsonPatchBuilder.remove("/passingAssertions/urn:li:assertion:test");

    result = assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    // Validate that we successfully removed, but did not disturb the other assertion.
    assertEquals(result.getPassingAssertions().size(), 1);
    assertEquals(result.getPassingAssertions().get(0), TEST_ASSERTION_URN_2);
  }

  @Test
  public void testAddLegacyFailingUrns() throws Exception {
    AssertionsSummaryTemplate assertionsSummaryTemplate = new AssertionsSummaryTemplate();
    AssertionsSummary baseSummary = assertionsSummaryTemplate.getDefault();

    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();

    jsonPatchBuilder.add("/failingAssertions/urn:li:assertion:test", TEST_ASSERTION_URN.toString());

    // Case 1: Initial population test
    AssertionsSummary result =
        assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    assertEquals(result.getFailingAssertions().size(), 1);
    assertEquals(result.getFailingAssertions().get(0), TEST_ASSERTION_URN);

    // Case 2: Test non-overwrite existing assertions using new assertion URN
    jsonPatchBuilder = Json.createPatchBuilder();

    jsonPatchBuilder.add(
        "/failingAssertions/urn:li:assertion:test2", TEST_ASSERTION_URN_2.toString());

    result = assertionsSummaryTemplate.applyPatch(result, jsonPatchBuilder.build());

    assertEquals(result.getFailingAssertions().size(), 2);
    assertEquals(result.getFailingAssertions().get(0), TEST_ASSERTION_URN);
    assertEquals(result.getFailingAssertions().get(1), TEST_ASSERTION_URN_2);
  }

  @Test
  public void testRemoveLegacyFailingUrns() throws Exception {
    AssertionsSummaryTemplate assertionsSummaryTemplate = new AssertionsSummaryTemplate();
    AssertionsSummary baseSummary = assertionsSummaryTemplate.getDefault();

    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();

    jsonPatchBuilder.remove("/failingAssertions/urn:li:assertion:test");

    // Case 1: Initial removal test - no changes
    AssertionsSummary result =
        assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    assertEquals(result.getFailingAssertions().size(), 0);

    // Case 2: Remove existing assertion
    jsonPatchBuilder = Json.createPatchBuilder();

    baseSummary = new AssertionsSummary();
    baseSummary.setFailingAssertions(new UrnArray(ImmutableList.of(TEST_ASSERTION_URN)));

    jsonPatchBuilder.remove("/failingAssertions/urn:li:assertion:test");

    result = assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    // Validate that we successfully removed.
    assertEquals(result.getFailingAssertions().size(), 0);

    // Case 2: Remove existing assertion, but don't touch unrelated one.
    jsonPatchBuilder = Json.createPatchBuilder();

    baseSummary = new AssertionsSummary();
    baseSummary.setFailingAssertions(
        new UrnArray(ImmutableList.of(TEST_ASSERTION_URN, TEST_ASSERTION_URN_2)));

    jsonPatchBuilder.remove("/failingAssertions/urn:li:assertion:test");

    result = assertionsSummaryTemplate.applyPatch(baseSummary, jsonPatchBuilder.build());

    // Validate that we successfully removed, but did not disturb the other assertion.
    assertEquals(result.getFailingAssertions().size(), 1);
    assertEquals(result.getFailingAssertions().get(0), TEST_ASSERTION_URN_2);
  }
}
