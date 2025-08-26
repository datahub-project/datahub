package com.linkedin.metadata.service;

import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.common.AssertionSummaryDetails;
import com.linkedin.common.AssertionSummaryDetailsArray;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AssertionsSummaryUtilsTest {

  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_ASSERTION_URN_2 = UrnUtils.getUrn("urn:li:assertion:test-2");
  private static final String TEST_ASSERTION_TYPE = "testType";

  @Test
  public void testRemoveAssertionFromFailingSummary() {
    // Case 1: Has the assertion in failing.
    AssertionsSummary summary =
        mockAssertionsSummary(
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN)),
            Collections.emptyList());
    AssertionsSummaryUtils.removeAssertionFromFailingSummary(TEST_ASSERTION_URN, summary);
    Assert.assertEquals(
        summary, mockAssertionsSummary(Collections.emptyList(), Collections.emptyList()));

    // Case 2: Has the assertion in passing.
    summary =
        mockAssertionsSummary(
            Collections.emptyList(),
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN)));
    AssertionsSummaryUtils.removeAssertionFromFailingSummary(TEST_ASSERTION_URN, summary);
    Assert.assertEquals(
        summary,
        mockAssertionsSummary(
            Collections.emptyList(),
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN))));

    // Case 3: Does not have the assertion at all.
    summary = mockAssertionsSummary(Collections.emptyList(), Collections.emptyList());
    AssertionsSummaryUtils.removeAssertionFromFailingSummary(TEST_ASSERTION_URN, summary);
    Assert.assertEquals(
        summary, mockAssertionsSummary(Collections.emptyList(), Collections.emptyList()));

    // Case 4: Has 2 items in list.
    summary =
        mockAssertionsSummary(
            ImmutableList.of(
                buildAssertionSummaryDetails(TEST_ASSERTION_URN),
                buildAssertionSummaryDetails(TEST_ASSERTION_URN_2)),
            Collections.emptyList());
    AssertionsSummaryUtils.removeAssertionFromFailingSummary(TEST_ASSERTION_URN, summary);
    Assert.assertEquals(
        summary,
        mockAssertionsSummary(
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN_2)),
            Collections.emptyList()));
  }

  @Test
  public void testRemoveAssertionFromPassingSummary() {
    // Case 1: Has the assertion in passing.
    AssertionsSummary summary =
        mockAssertionsSummary(
            Collections.emptyList(),
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN)));
    AssertionsSummaryUtils.removeAssertionFromPassingSummary(TEST_ASSERTION_URN, summary);
    Assert.assertEquals(
        summary, mockAssertionsSummary(Collections.emptyList(), Collections.emptyList()));

    // Case 2: Has the assertion in failing.
    summary =
        mockAssertionsSummary(
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN)),
            Collections.emptyList());
    AssertionsSummaryUtils.removeAssertionFromPassingSummary(TEST_ASSERTION_URN, summary);
    Assert.assertEquals(
        summary,
        mockAssertionsSummary(
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN)),
            Collections.emptyList()));

    // Case 3: Does not have the assertion at all.
    summary = mockAssertionsSummary(Collections.emptyList(), Collections.emptyList());
    AssertionsSummaryUtils.removeAssertionFromPassingSummary(TEST_ASSERTION_URN, summary);
    Assert.assertEquals(
        summary, mockAssertionsSummary(Collections.emptyList(), Collections.emptyList()));

    // Case 4: Has 2 items in list.
    summary =
        mockAssertionsSummary(
            Collections.emptyList(),
            ImmutableList.of(
                buildAssertionSummaryDetails(TEST_ASSERTION_URN),
                buildAssertionSummaryDetails(TEST_ASSERTION_URN_2)));
    AssertionsSummaryUtils.removeAssertionFromPassingSummary(TEST_ASSERTION_URN, summary);
    Assert.assertEquals(
        summary,
        mockAssertionsSummary(
            Collections.emptyList(),
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN_2))));
  }

  @Test
  public void testAddAssertionToPassingSummary() {
    // Case 1: Has an assertion in passing.
    AssertionsSummary summary =
        mockAssertionsSummary(
            Collections.emptyList(),
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN)));
    AssertionsSummaryUtils.addAssertionToPassingSummary(
        buildAssertionSummaryDetails(TEST_ASSERTION_URN_2), summary);
    AssertionsSummary expected =
        mockAssertionsSummary(
            Collections.emptyList(),
            ImmutableList.of(
                buildAssertionSummaryDetails(TEST_ASSERTION_URN),
                buildAssertionSummaryDetails(TEST_ASSERTION_URN_2)));
    Assert.assertEquals(
        new HashSet<>(summary.getPassingAssertionDetails()),
        new HashSet<>(expected.getPassingAssertionDetails())); // Set comparison
    Assert.assertEquals(
        new HashSet<>(summary.getFailingAssertionDetails()),
        new HashSet<>(expected.getFailingAssertionDetails())); // Set comparison

    // Case 2: Has an assertion in failing.
    summary =
        mockAssertionsSummary(
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN)),
            Collections.emptyList());
    AssertionsSummaryUtils.addAssertionToPassingSummary(
        buildAssertionSummaryDetails(TEST_ASSERTION_URN_2), summary);
    Assert.assertEquals(
        summary,
        mockAssertionsSummary(
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN)),
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN_2))));

    // Case 3: Does not have any assertions yet
    summary = mockAssertionsSummary(Collections.emptyList(), Collections.emptyList());
    AssertionsSummaryUtils.addAssertionToPassingSummary(
        buildAssertionSummaryDetails(TEST_ASSERTION_URN), summary);
    Assert.assertEquals(
        summary,
        mockAssertionsSummary(
            Collections.emptyList(),
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN))));

    // Case 4: Duplicate additions - already has the same assertion
    summary =
        mockAssertionsSummary(
            Collections.emptyList(),
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN)));
    AssertionsSummaryUtils.addAssertionToPassingSummary(
        buildAssertionSummaryDetails(TEST_ASSERTION_URN), summary);
    Assert.assertEquals(
        summary,
        mockAssertionsSummary(
            Collections.emptyList(),
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN))));
  }

  @Test
  public void testAddAssertionToFailingSummary() {
    // Case 1: Has an assertion in failing.
    AssertionsSummary summary =
        mockAssertionsSummary(
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN)),
            Collections.emptyList());
    AssertionsSummaryUtils.addAssertionToFailingSummary(
        buildAssertionSummaryDetails(TEST_ASSERTION_URN_2), summary);
    AssertionsSummary expected =
        mockAssertionsSummary(
            ImmutableList.of(
                buildAssertionSummaryDetails(TEST_ASSERTION_URN),
                buildAssertionSummaryDetails(TEST_ASSERTION_URN_2)),
            Collections.emptyList());
    Assert.assertEquals(
        new HashSet<>(summary.getPassingAssertionDetails()),
        new HashSet<>(expected.getPassingAssertionDetails())); // Set comparison
    Assert.assertEquals(
        new HashSet<>(summary.getFailingAssertionDetails()),
        new HashSet<>(expected.getFailingAssertionDetails())); // Set comparison

    // Case 2: Has an assertion in passing.
    summary =
        mockAssertionsSummary(
            Collections.emptyList(),
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN)));
    AssertionsSummaryUtils.addAssertionToFailingSummary(
        buildAssertionSummaryDetails(TEST_ASSERTION_URN_2), summary);
    Assert.assertEquals(
        summary,
        mockAssertionsSummary(
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN_2)),
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN))));

    // Case 3: Does not have any assertions yet
    summary = mockAssertionsSummary(Collections.emptyList(), Collections.emptyList());
    AssertionsSummaryUtils.addAssertionToFailingSummary(
        buildAssertionSummaryDetails(TEST_ASSERTION_URN), summary);
    Assert.assertEquals(
        summary,
        mockAssertionsSummary(
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN)),
            Collections.emptyList()));

    // Case 4: Duplicate additions - already has the same assertion
    summary =
        mockAssertionsSummary(
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN)),
            Collections.emptyList());
    AssertionsSummaryUtils.addAssertionToFailingSummary(
        buildAssertionSummaryDetails(TEST_ASSERTION_URN), summary);
    Assert.assertEquals(
        summary,
        mockAssertionsSummary(
            ImmutableList.of(buildAssertionSummaryDetails(TEST_ASSERTION_URN)),
            Collections.emptyList()));
  }

  private AssertionsSummary mockAssertionsSummary(
      final List<AssertionSummaryDetails> failingAssertions,
      final List<AssertionSummaryDetails> passingAssertions) {
    return new AssertionsSummary()
        .setPassingAssertionDetails(new AssertionSummaryDetailsArray(passingAssertions))
        .setFailingAssertionDetails(new AssertionSummaryDetailsArray(failingAssertions));
  }

  private AssertionSummaryDetails buildAssertionSummaryDetails(Urn assertionUrn) {
    return buildAssertionSummaryDetails(assertionUrn, TEST_ASSERTION_TYPE, 1L);
  }

  private AssertionSummaryDetails buildAssertionSummaryDetails(
      Urn assertionUrn, String assertionType, Long lastResultAt) {
    AssertionSummaryDetails details = new AssertionSummaryDetails();
    details.setUrn(assertionUrn);
    details.setType(assertionType);
    details.setLastResultAt(lastResultAt, SetMode.IGNORE_NULL);
    details.setSource(AssertionSourceType.EXTERNAL.toString());
    return details;
  }
}
