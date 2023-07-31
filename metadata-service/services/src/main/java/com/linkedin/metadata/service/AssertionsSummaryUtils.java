package com.linkedin.metadata.service;

import com.linkedin.common.AssertionSummaryDetails;
import com.linkedin.common.AssertionSummaryDetailsArray;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class AssertionsSummaryUtils {

  public static void removeAssertionFromFailingSummary(@Nonnull final Urn assertionUrn, @Nonnull final AssertionsSummary summary) {
    // Legacy - remove from deprecated field.
    if (summary.hasFailingAssertions()) {
      final Set<Urn> failingAssertions = new HashSet<>(summary.getFailingAssertions());
      failingAssertions.remove(assertionUrn);
      summary.setFailingAssertions(new UrnArray(new ArrayList<>(failingAssertions)));
    }
    // New - remove from new field.
    if (summary.hasFailingAssertionDetails()) {
      final Set<AssertionSummaryDetails> filteredDetails = summary.getFailingAssertionDetails().stream()
          .filter(details -> !assertionUrn.equals(details.getUrn()))
          .collect(Collectors.toSet());
      summary.setFailingAssertionDetails(new AssertionSummaryDetailsArray(new ArrayList<>(filteredDetails)));
    }
  }

  public static void removeAssertionFromPassingSummary(@Nonnull final Urn assertionUrn,  @Nonnull final AssertionsSummary summary) {
    // Legacy - remove the deprecated field.
    if (summary.hasPassingAssertions()) {
      final Set<Urn> passingAssertions = new HashSet<>(summary.getPassingAssertions());
      passingAssertions.remove(assertionUrn);
      summary.setPassingAssertions(new UrnArray(new ArrayList<>(passingAssertions)));
    }
    // New - remove from the new field.
    if (summary.hasPassingAssertionDetails()) {
      final Set<AssertionSummaryDetails> filteredDetails = summary.getPassingAssertionDetails().stream()
          .filter(details -> !assertionUrn.equals(details.getUrn()))
          .collect(Collectors.toSet());
      summary.setPassingAssertionDetails(new AssertionSummaryDetailsArray(new ArrayList<>(filteredDetails)));
    }
  }

  public static void addAssertionToFailingSummary(@Nonnull final AssertionSummaryDetails details,  @Nonnull final AssertionsSummary summary) {
    final List<AssertionSummaryDetails> existingDetails = summary.getFailingAssertionDetails();
    final List<AssertionSummaryDetails> newDetails = existingDetails.stream()
        .filter(existing -> !details.getUrn().equals(existing.getUrn()))
        .collect(Collectors.toList());
    newDetails.add(details);
    summary.setFailingAssertionDetails(new AssertionSummaryDetailsArray(newDetails));
  }

  public static void addAssertionToPassingSummary(@Nonnull final AssertionSummaryDetails details, @Nonnull final AssertionsSummary summary) {
    final List<AssertionSummaryDetails> existingDetails = summary.getPassingAssertionDetails();
    final List<AssertionSummaryDetails> newDetails = existingDetails.stream()
        .filter(existing -> !details.getUrn().equals(existing.getUrn()))
        .collect(Collectors.toList());
    newDetails.add(details);
    summary.setPassingAssertionDetails(new AssertionSummaryDetailsArray(newDetails));
  }

  private AssertionsSummaryUtils() { }
}
