package com.linkedin.metadata.kafka.hook.assertion;

import com.linkedin.assertion.AssertionAnalyticsRunEvent;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import org.mockito.ArgumentMatcher;

public class AssertionAnalyticsRunEventHookRunEventMatcher
    implements ArgumentMatcher<MetadataChangeProposal> {

  private MetadataChangeProposal left;

  public AssertionAnalyticsRunEventHookRunEventMatcher(MetadataChangeProposal left) {
    this.left = left;
  }

  @Override
  public boolean matches(MetadataChangeProposal right) {
    return left.getEntityType().equals(right.getEntityType())
        && left.getAspectName().equals(right.getAspectName())
        && left.getChangeType().equals(right.getChangeType())
        && runEventsMatch(left.getAspect(), right.getAspect());
  }

  private boolean runEventsMatch(GenericAspect left, GenericAspect right) {

    AssertionAnalyticsRunEvent leftProps =
        GenericRecordUtils.deserializeAspect(
            left.getValue(), "application/json", AssertionAnalyticsRunEvent.class);

    AssertionAnalyticsRunEvent rightProps =
        GenericRecordUtils.deserializeAspect(
            right.getValue(), "application/json", AssertionAnalyticsRunEvent.class);

    // Omit timestamp comparison.
    return leftProps.toString().equals(rightProps.toString());
  }
}
