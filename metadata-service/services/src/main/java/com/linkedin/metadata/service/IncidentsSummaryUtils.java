package com.linkedin.metadata.service;

import com.linkedin.common.IncidentSummaryDetails;
import com.linkedin.common.IncidentSummaryDetailsArray;
import com.linkedin.common.IncidentsSummary;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class IncidentsSummaryUtils {

  public static void removeIncidentFromResolvedSummary(
      @Nonnull final Urn incidentUrn, @Nonnull final IncidentsSummary summary) {
    // Legacy - remove from deprecated field.
    if (summary.hasResolvedIncidents()) {
      final Set<Urn> resolvedIncidents = new HashSet<>(summary.getResolvedIncidents());
      resolvedIncidents.remove(incidentUrn);
      summary.setResolvedIncidents(new UrnArray(new ArrayList<>(resolvedIncidents)));
    }
    // New - remove from new field.
    if (summary.hasResolvedIncidentDetails()) {
      final Set<IncidentSummaryDetails> filteredDetails =
          summary.getResolvedIncidentDetails().stream()
              .filter(details -> !incidentUrn.equals(details.getUrn()))
              .collect(Collectors.toSet());
      summary.setResolvedIncidentDetails(
          new IncidentSummaryDetailsArray(new ArrayList<>(filteredDetails)));
    }
  }

  public static void removeIncidentFromActiveSummary(
      @Nonnull final Urn incidentUrn, @Nonnull final IncidentsSummary summary) {
    // Legacy - remove the deprecated field.
    if (summary.hasActiveIncidents()) {
      final Set<Urn> activeIncidents = new HashSet<>(summary.getActiveIncidents());
      activeIncidents.remove(incidentUrn);
      summary.setActiveIncidents(new UrnArray(new ArrayList<>(activeIncidents)));
    }
    // New - remove from the new field.
    if (summary.hasActiveIncidentDetails()) {
      final Set<IncidentSummaryDetails> filteredDetails =
          summary.getActiveIncidentDetails().stream()
              .filter(details -> !incidentUrn.equals(details.getUrn()))
              .collect(Collectors.toSet());
      summary.setActiveIncidentDetails(
          new IncidentSummaryDetailsArray(new ArrayList<>(filteredDetails)));
    }
  }

  public static void addIncidentToResolvedSummary(
      @Nonnull final IncidentSummaryDetails details,
      @Nonnull final IncidentsSummary summary,
      int maxIncidentHistory) {
    final List<IncidentSummaryDetails> existingDetails = summary.getResolvedIncidentDetails();
    final List<IncidentSummaryDetails> newDetails =
        existingDetails.stream()
            .filter(existing -> !details.getUrn().equals(existing.getUrn()))
            .sorted(Comparator.comparing(IncidentSummaryDetails::getCreatedAt))
            .collect(Collectors.toList());
    while (newDetails.size() >= maxIncidentHistory && !newDetails.isEmpty()) {
      // Removes oldest entry until size is less than max size
      newDetails.remove(0);
    }
    newDetails.add(details);
    summary.setResolvedIncidentDetails(new IncidentSummaryDetailsArray(newDetails));
  }

  public static void addIncidentToActiveSummary(
      @Nonnull final IncidentSummaryDetails details,
      @Nonnull final IncidentsSummary summary,
      int maxIncidentHistory) {
    final List<IncidentSummaryDetails> existingDetails = summary.getActiveIncidentDetails();
    final List<IncidentSummaryDetails> newDetails =
        existingDetails.stream()
            .filter(existing -> !details.getUrn().equals(existing.getUrn()))
            .sorted(Comparator.comparing(IncidentSummaryDetails::getCreatedAt))
            .collect(Collectors.toList());
    while (newDetails.size() >= maxIncidentHistory && !newDetails.isEmpty()) {
      // Removes oldest entry until size is less than max size
      newDetails.remove(0);
    }
    newDetails.add(details);
    summary.setActiveIncidentDetails(new IncidentSummaryDetailsArray(newDetails));
  }

  private IncidentsSummaryUtils() {}
}
