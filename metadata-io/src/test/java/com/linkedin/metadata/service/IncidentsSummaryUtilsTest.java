package com.linkedin.metadata.service;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.IncidentSummaryDetails;
import com.linkedin.common.IncidentSummaryDetailsArray;
import com.linkedin.common.IncidentsSummary;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IncidentsSummaryUtilsTest {

  private static final Urn TEST_INCIDENT_URN = UrnUtils.getUrn("urn:li:incident:test");
  private static final Urn TEST_INCIDENT_URN_2 = UrnUtils.getUrn("urn:li:incident:test-2");
  private static final String TEST_INCIDENT_TYPE = "testType";

  @Test
  public void testRemoveIncidentFromResolvedSummaryLegacy() {
    // Case 1: Has the incident in resolved.
    IncidentsSummary summary =
        mockIncidentsSummaryLegacy(ImmutableList.of(TEST_INCIDENT_URN), Collections.emptyList());
    IncidentsSummaryUtils.removeIncidentFromResolvedSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary, mockIncidentsSummaryLegacy(Collections.emptyList(), Collections.emptyList()));

    // Case 2: Has the incident in active.
    summary =
        mockIncidentsSummaryLegacy(Collections.emptyList(), ImmutableList.of(TEST_INCIDENT_URN));
    IncidentsSummaryUtils.removeIncidentFromResolvedSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary,
        mockIncidentsSummaryLegacy(Collections.emptyList(), ImmutableList.of(TEST_INCIDENT_URN)));

    // Case 3: Does not have the incident at all.
    summary = mockIncidentsSummaryLegacy(Collections.emptyList(), Collections.emptyList());
    IncidentsSummaryUtils.removeIncidentFromResolvedSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary, mockIncidentsSummaryLegacy(Collections.emptyList(), Collections.emptyList()));

    // Case 4: Has 2 items in list.
    summary =
        mockIncidentsSummaryLegacy(
            ImmutableList.of(TEST_INCIDENT_URN, TEST_INCIDENT_URN_2), Collections.emptyList());
    IncidentsSummaryUtils.removeIncidentFromResolvedSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary,
        mockIncidentsSummaryLegacy(ImmutableList.of(TEST_INCIDENT_URN_2), Collections.emptyList()));
  }

  @Test
  public void testRemoveIncidentFromActiveSummaryLegacy() {
    // Case 1: Has the incident in active.
    IncidentsSummary summary =
        mockIncidentsSummaryLegacy(Collections.emptyList(), ImmutableList.of(TEST_INCIDENT_URN));
    IncidentsSummaryUtils.removeIncidentFromActiveSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary, mockIncidentsSummaryLegacy(Collections.emptyList(), Collections.emptyList()));

    // Case 2: Has the incident in resolved.
    summary =
        mockIncidentsSummaryLegacy(ImmutableList.of(TEST_INCIDENT_URN), Collections.emptyList());
    IncidentsSummaryUtils.removeIncidentFromActiveSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary,
        mockIncidentsSummaryLegacy(ImmutableList.of(TEST_INCIDENT_URN), Collections.emptyList()));

    // Case 3: Does not have the incident at all.
    summary = mockIncidentsSummaryLegacy(Collections.emptyList(), Collections.emptyList());
    IncidentsSummaryUtils.removeIncidentFromActiveSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary, mockIncidentsSummaryLegacy(Collections.emptyList(), Collections.emptyList()));

    // Case 4: Has 2 items in list.
    summary =
        mockIncidentsSummaryLegacy(
            Collections.emptyList(), ImmutableList.of(TEST_INCIDENT_URN, TEST_INCIDENT_URN_2));
    IncidentsSummaryUtils.removeIncidentFromActiveSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary,
        mockIncidentsSummaryLegacy(Collections.emptyList(), ImmutableList.of(TEST_INCIDENT_URN_2)));
  }

  @Test
  public void testRemoveIncidentFromResolvedSummary() {
    // Case 1: Has the incident in resolved details.
    IncidentsSummary summary =
        mockIncidentsSummary(
            ImmutableList.of(buildIncidentDetails(TEST_INCIDENT_URN, TEST_INCIDENT_TYPE)),
            Collections.emptyList());
    IncidentsSummaryUtils.removeIncidentFromResolvedSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary, mockIncidentsSummary(Collections.emptyList(), Collections.emptyList()));

    // Case 2: Has the incident in active.
    summary =
        mockIncidentsSummary(
            Collections.emptyList(),
            ImmutableList.of(buildIncidentDetails(TEST_INCIDENT_URN, TEST_INCIDENT_TYPE)));
    IncidentsSummaryUtils.removeIncidentFromResolvedSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary,
        mockIncidentsSummary(
            Collections.emptyList(),
            ImmutableList.of(buildIncidentDetails(TEST_INCIDENT_URN, TEST_INCIDENT_TYPE))));

    // Case 3: Does not have the incident at all.
    summary = mockIncidentsSummary(Collections.emptyList(), Collections.emptyList());
    IncidentsSummaryUtils.removeIncidentFromResolvedSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary, mockIncidentsSummary(Collections.emptyList(), Collections.emptyList()));

    // Case 4: Has 2 items in list.
    summary =
        mockIncidentsSummary(
            ImmutableList.of(
                buildIncidentDetails(TEST_INCIDENT_URN, TEST_INCIDENT_TYPE),
                buildIncidentDetails(TEST_INCIDENT_URN_2, TEST_INCIDENT_TYPE)),
            Collections.emptyList());
    IncidentsSummaryUtils.removeIncidentFromResolvedSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary,
        mockIncidentsSummary(
            ImmutableList.of(buildIncidentDetails(TEST_INCIDENT_URN_2, TEST_INCIDENT_TYPE)),
            Collections.emptyList()));
  }

  @Test
  public void testRemoveIncidentFromActiveSummary() {
    // Case 1: Has the incident in active.
    IncidentsSummary summary =
        mockIncidentsSummary(
            Collections.emptyList(),
            ImmutableList.of(buildIncidentDetails(TEST_INCIDENT_URN, TEST_INCIDENT_TYPE)));
    IncidentsSummaryUtils.removeIncidentFromActiveSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary, mockIncidentsSummary(Collections.emptyList(), Collections.emptyList()));

    // Case 2: Has the incident in resolved.
    summary =
        mockIncidentsSummary(
            ImmutableList.of(buildIncidentDetails(TEST_INCIDENT_URN, TEST_INCIDENT_TYPE)),
            Collections.emptyList());
    IncidentsSummaryUtils.removeIncidentFromActiveSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary,
        mockIncidentsSummary(
            ImmutableList.of(buildIncidentDetails(TEST_INCIDENT_URN, TEST_INCIDENT_TYPE)),
            Collections.emptyList()));

    // Case 3: Does not have the incident at all.
    summary = mockIncidentsSummary(Collections.emptyList(), Collections.emptyList());
    IncidentsSummaryUtils.removeIncidentFromActiveSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary, mockIncidentsSummary(Collections.emptyList(), Collections.emptyList()));

    // Case 4: Has 2 items in list.
    summary =
        mockIncidentsSummary(
            Collections.emptyList(),
            ImmutableList.of(
                buildIncidentDetails(TEST_INCIDENT_URN, TEST_INCIDENT_TYPE),
                buildIncidentDetails(TEST_INCIDENT_URN_2, TEST_INCIDENT_TYPE)));
    IncidentsSummaryUtils.removeIncidentFromActiveSummary(TEST_INCIDENT_URN, summary);
    Assert.assertEquals(
        summary,
        mockIncidentsSummary(
            Collections.emptyList(),
            ImmutableList.of(buildIncidentDetails(TEST_INCIDENT_URN_2, TEST_INCIDENT_TYPE))));
  }

  @Test
  public void testAddIncidentToActiveSummary() {
    // Case 1: Has an incident in active.
    IncidentSummaryDetails existingDetails =
        buildIncidentDetails(TEST_INCIDENT_URN, TEST_INCIDENT_TYPE);
    IncidentsSummary summary =
        mockIncidentsSummary(Collections.emptyList(), ImmutableList.of(existingDetails));
    IncidentSummaryDetails newDetails =
        buildIncidentDetails(TEST_INCIDENT_URN_2, TEST_INCIDENT_TYPE);
    IncidentsSummaryUtils.addIncidentToActiveSummary(newDetails, summary, 100);
    IncidentsSummary expected =
        mockIncidentsSummary(
            Collections.emptyList(), ImmutableList.of(existingDetails, newDetails));
    Assert.assertEquals(
        new HashSet<>(summary.getActiveIncidentDetails()),
        new HashSet<>(expected.getActiveIncidentDetails())); // Set comparison
    Assert.assertEquals(
        new HashSet<>(summary.getResolvedIncidentDetails()),
        new HashSet<>(expected.getResolvedIncidentDetails())); // Set comparison

    // Case 2: Has an incident in resolved.
    summary = mockIncidentsSummary(ImmutableList.of(existingDetails), Collections.emptyList());
    IncidentsSummaryUtils.addIncidentToActiveSummary(newDetails, summary, 100);
    Assert.assertEquals(
        summary,
        mockIncidentsSummary(ImmutableList.of(existingDetails), ImmutableList.of(newDetails)));

    // Case 3: Does not have any incidents yet
    summary = mockIncidentsSummary(Collections.emptyList(), Collections.emptyList());
    IncidentsSummaryUtils.addIncidentToActiveSummary(newDetails, summary, 100);
    Assert.assertEquals(
        summary, mockIncidentsSummary(Collections.emptyList(), ImmutableList.of(newDetails)));

    // Case 4: Duplicate additions - already has the same incident in the list
    summary = mockIncidentsSummary(Collections.emptyList(), ImmutableList.of(existingDetails));
    newDetails = buildIncidentDetails(TEST_INCIDENT_URN, "type2");
    IncidentsSummaryUtils.addIncidentToActiveSummary(newDetails, summary, 100);
    Assert.assertEquals(
        summary, mockIncidentsSummary(Collections.emptyList(), ImmutableList.of(newDetails)));

    // Test out max size, removes old and adds in new
    summary = mockIncidentsSummary(Collections.emptyList(), ImmutableList.of(existingDetails));
    newDetails = buildIncidentDetails(TEST_INCIDENT_URN_2, TEST_INCIDENT_TYPE);
    IncidentsSummaryUtils.addIncidentToActiveSummary(newDetails, summary, 1);
    Assert.assertEquals(
        summary, mockIncidentsSummary(Collections.emptyList(), ImmutableList.of(newDetails)));
    Assert.assertEquals(summary.getActiveIncidentDetails().size(), 1);
  }

  @Test
  public void testAddIncidentToResolvedSummary() {
    // Case 1: Has an incident in resolved.
    IncidentSummaryDetails existingDetails =
        buildIncidentDetails(TEST_INCIDENT_URN, TEST_INCIDENT_TYPE);
    IncidentsSummary summary =
        mockIncidentsSummary(ImmutableList.of(existingDetails), Collections.emptyList());
    IncidentSummaryDetails newDetails =
        buildIncidentDetails(TEST_INCIDENT_URN_2, TEST_INCIDENT_TYPE);
    IncidentsSummaryUtils.addIncidentToResolvedSummary(newDetails, summary, 100);
    IncidentsSummary expected =
        mockIncidentsSummary(
            ImmutableList.of(existingDetails, newDetails), Collections.emptyList());
    Assert.assertEquals(
        new HashSet<>(summary.getActiveIncidentDetails()),
        new HashSet<>(expected.getActiveIncidentDetails())); // Set comparison
    Assert.assertEquals(
        new HashSet<>(summary.getResolvedIncidentDetails()),
        new HashSet<>(expected.getResolvedIncidentDetails())); // Set comparison

    // Case 2: Has an incident in active.
    summary = mockIncidentsSummary(Collections.emptyList(), ImmutableList.of(existingDetails));
    IncidentsSummaryUtils.addIncidentToResolvedSummary(newDetails, summary, 100);
    Assert.assertEquals(
        summary,
        mockIncidentsSummary(ImmutableList.of(newDetails), ImmutableList.of(existingDetails)));

    // Case 3: Does not have any incidents yet
    summary = mockIncidentsSummary(Collections.emptyList(), Collections.emptyList());
    IncidentsSummaryUtils.addIncidentToResolvedSummary(newDetails, summary, 100);
    Assert.assertEquals(
        summary, mockIncidentsSummary(ImmutableList.of(newDetails), Collections.emptyList()));

    // Case 4: Duplicate additions - already has the same incident
    summary = mockIncidentsSummary(ImmutableList.of(existingDetails), Collections.emptyList());
    newDetails = buildIncidentDetails(TEST_INCIDENT_URN, "type2");
    IncidentsSummaryUtils.addIncidentToResolvedSummary(newDetails, summary, 100);
    Assert.assertEquals(
        summary, mockIncidentsSummary(ImmutableList.of(newDetails), Collections.emptyList()));

    // Test out max size, removes old and adds in new
    summary = mockIncidentsSummary(ImmutableList.of(existingDetails), Collections.emptyList());
    newDetails = buildIncidentDetails(TEST_INCIDENT_URN_2, TEST_INCIDENT_TYPE);
    IncidentsSummaryUtils.addIncidentToResolvedSummary(newDetails, summary, 1);
    Assert.assertEquals(
        summary, mockIncidentsSummary(ImmutableList.of(newDetails), Collections.emptyList()));
    Assert.assertEquals(summary.getResolvedIncidentDetails().size(), 1);
  }

  private IncidentsSummary mockIncidentsSummaryLegacy(
      final List<Urn> resolvedIncidents, final List<Urn> activeIncidents) {
    return new IncidentsSummary()
        .setResolvedIncidents(new UrnArray(resolvedIncidents))
        .setActiveIncidents(new UrnArray(activeIncidents));
  }

  private IncidentsSummary mockIncidentsSummary(
      final List<IncidentSummaryDetails> resolvedIncidents,
      final List<IncidentSummaryDetails> activeIncidents) {
    return new IncidentsSummary()
        .setResolvedIncidentDetails(new IncidentSummaryDetailsArray(resolvedIncidents))
        .setActiveIncidentDetails(new IncidentSummaryDetailsArray(activeIncidents));
  }

  private IncidentSummaryDetails buildIncidentDetails(Urn incidentUrn, String incidentType) {
    return buildIncidentDetails(incidentUrn, incidentType, 1, 0L, 1L);
  }

  private IncidentSummaryDetails buildIncidentDetails(
      Urn incidentUrn, String incidentType, Integer priority) {
    return buildIncidentDetails(incidentUrn, incidentType, priority, 0L, 1L);
  }

  private IncidentSummaryDetails buildIncidentDetails(
      Urn incidentUrn, String incidentType, Integer priority, Long createdAt, Long resolvedAt) {
    IncidentSummaryDetails details = new IncidentSummaryDetails();
    details.setUrn(incidentUrn);
    details.setType(incidentType);
    details.setPriority(priority, SetMode.IGNORE_NULL);
    details.setCreatedAt(createdAt, SetMode.IGNORE_NULL);
    details.setResolvedAt(resolvedAt, SetMode.IGNORE_NULL);
    return details;
  }
}
