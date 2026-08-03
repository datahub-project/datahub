package com.linkedin.datahub.graphql.resolvers.incident;

import com.linkedin.incident.IncidentAssignee;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.List;
import java.util.Objects;
import org.mockito.ArgumentMatcher;

/** Matcher for incident MCPs that checks both field values and optional-field presence. */
public class IncidentInfoMatcher implements ArgumentMatcher<MetadataChangeProposal> {

  private final MetadataChangeProposal expected;

  public IncidentInfoMatcher(MetadataChangeProposal expected) {
    this.expected = expected;
  }

  @Override
  public boolean matches(MetadataChangeProposal actual) {
    return expected.getEntityType().equals(actual.getEntityType())
        && expected.getAspectName().equals(actual.getAspectName())
        && expected.getChangeType().equals(actual.getChangeType())
        && incidentInfoMatches(expected.getAspect(), actual.getAspect());
  }

  private boolean incidentInfoMatches(GenericAspect expectedAspect, GenericAspect actualAspect) {
    IncidentInfo expectedInfo = deserialize(expectedAspect);
    IncidentInfo actualInfo = deserialize(actualAspect);

    if (!matchesOptional(
        expectedInfo.hasCustomType(),
        actualInfo.hasCustomType(),
        expectedInfo.getCustomType(),
        actualInfo.getCustomType())) {
      return false;
    }
    if (!matchesOptional(
        expectedInfo.hasTitle(),
        actualInfo.hasTitle(),
        expectedInfo.getTitle(),
        actualInfo.getTitle())) {
      return false;
    }
    if (!matchesOptional(
        expectedInfo.hasDescription(),
        actualInfo.hasDescription(),
        expectedInfo.getDescription(),
        actualInfo.getDescription())) {
      return false;
    }
    if (!matchesOptional(
        expectedInfo.hasType(),
        actualInfo.hasType(),
        expectedInfo.getType(),
        actualInfo.getType())) {
      return false;
    }
    if (!matchesOptional(
        expectedInfo.hasStartedAt(),
        actualInfo.hasStartedAt(),
        expectedInfo.getStartedAt(),
        actualInfo.getStartedAt())) {
      return false;
    }
    if (!matchesOptional(
        expectedInfo.hasPriority(),
        actualInfo.hasPriority(),
        expectedInfo.getPriority(),
        actualInfo.getPriority())) {
      return false;
    }
    if (!matchesOptional(
        expectedInfo.hasEntities(),
        actualInfo.hasEntities(),
        expectedInfo.getEntities(),
        actualInfo.getEntities())) {
      return false;
    }
    if (!matchesAssignees(expectedInfo, actualInfo)
        || !matchesStatus(expectedInfo, actualInfo)
        || !matchesSource(expectedInfo, actualInfo)) {
      return false;
    }

    return true;
  }

  private boolean matchesAssignees(IncidentInfo expectedInfo, IncidentInfo actualInfo) {
    if (expectedInfo.hasAssignees() != actualInfo.hasAssignees()) {
      return false;
    }
    if (!expectedInfo.hasAssignees()) {
      return true;
    }
    List<IncidentAssignee> expectedAssignees = expectedInfo.getAssignees();
    List<IncidentAssignee> actualAssignees = actualInfo.getAssignees();
    if (expectedAssignees.size() != actualAssignees.size()) {
      return false;
    }
    for (int i = 0; i < expectedAssignees.size(); i++) {
      if (!Objects.equals(expectedAssignees.get(i).getActor(), actualAssignees.get(i).getActor())) {
        return false;
      }
    }
    return true;
  }

  private boolean matchesStatus(IncidentInfo expectedInfo, IncidentInfo actualInfo) {
    if (expectedInfo.hasStatus() != actualInfo.hasStatus()) {
      return false;
    }
    if (!expectedInfo.hasStatus()) {
      return true;
    }
    IncidentStatus expectedStatus = expectedInfo.getStatus();
    IncidentStatus actualStatus = actualInfo.getStatus();
    return matchesOptional(
            expectedStatus.hasStage(),
            actualStatus.hasStage(),
            expectedStatus.getStage(),
            actualStatus.getStage())
        && matchesOptional(
            expectedStatus.hasState(),
            actualStatus.hasState(),
            expectedStatus.getState(),
            actualStatus.getState())
        && matchesOptional(
            expectedStatus.hasMessage(),
            actualStatus.hasMessage(),
            expectedStatus.getMessage(),
            actualStatus.getMessage());
  }

  private boolean matchesSource(IncidentInfo expectedInfo, IncidentInfo actualInfo) {
    if (expectedInfo.hasSource() != actualInfo.hasSource()) {
      return false;
    }
    if (!expectedInfo.hasSource()) {
      return true;
    }
    IncidentSource expectedSource = expectedInfo.getSource();
    IncidentSource actualSource = actualInfo.getSource();
    return matchesOptional(
            expectedSource.hasType(),
            actualSource.hasType(),
            expectedSource.getType(),
            actualSource.getType())
        && matchesOptional(
            expectedSource.hasSourceUrn(),
            actualSource.hasSourceUrn(),
            expectedSource.getSourceUrn(),
            actualSource.getSourceUrn());
  }

  private static boolean matchesOptional(
      boolean expectedPresent, boolean actualPresent, Object expectedValue, Object actualValue) {
    return expectedPresent == actualPresent
        && (!expectedPresent || Objects.equals(expectedValue, actualValue));
  }

  private static IncidentInfo deserialize(GenericAspect aspect) {
    return GenericRecordUtils.deserializeAspect(
        aspect.getValue(), "application/json", IncidentInfo.class);
  }
}
