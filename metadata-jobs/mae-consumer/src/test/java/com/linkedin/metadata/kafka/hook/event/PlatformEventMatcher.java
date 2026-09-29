package com.linkedin.metadata.kafka.hook.event;

import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import com.linkedin.platform.event.v1.RelationshipChangeEvent;
import javax.annotation.Nonnull;
import org.mockito.ArgumentMatcher;

public class PlatformEventMatcher implements ArgumentMatcher<PlatformEvent> {
  private final PlatformEvent _expected;

  public PlatformEventMatcher(@Nonnull final PlatformEvent expected) {
    _expected = expected;
  }

  @Override
  public boolean matches(@Nonnull final PlatformEvent actual) {
    return _expected.getName().equals(actual.getName())
        && _expected
            .getHeader()
            .getTimestampMillis()
            .equals(actual.getHeader().getTimestampMillis())
        && payloadMatches(actual);
  }

  public boolean payloadMatches(@Nonnull final PlatformEvent actual) {
    if (actual.getName().equals(Constants.CHANGE_EVENT_PLATFORM_EVENT_NAME)) {
      return entityChangepayloadMatches(actual);
    } else if (actual.getName().equals(Constants.RELATIONSHIP_PLATFORM_EVENT_NAME)) {
      return relationshipChangepayloadMatches(actual);
    }
    return false;
  }

  public boolean relationshipChangepayloadMatches(@Nonnull final PlatformEvent actual) {
    final RelationshipChangeEvent expectedChangeEvent =
        GenericRecordUtils.deserializePayload(
            _expected.getPayload().getValue(), RelationshipChangeEvent.class);
    final RelationshipChangeEvent actualChangeEvent =
        GenericRecordUtils.deserializePayload(
            actual.getPayload().getValue(), RelationshipChangeEvent.class);
    boolean requiredFieldsMatch =
        expectedChangeEvent.getOperation().equals(actualChangeEvent.getOperation())
            && expectedChangeEvent.getSourceUrn().equals(actualChangeEvent.getSourceUrn())
            && expectedChangeEvent.getDestinationUrn().equals(actualChangeEvent.getDestinationUrn())
            && expectedChangeEvent.getAuditStamp().equals(actualChangeEvent.getAuditStamp());
    return requiredFieldsMatch;
  }

  public boolean entityChangepayloadMatches(@Nonnull final PlatformEvent actual) {
    final EntityChangeEvent expectedChangeEvent =
        GenericRecordUtils.deserializePayload(
            _expected.getPayload().getValue(), EntityChangeEvent.class);
    final EntityChangeEvent actualChangeEvent =
        GenericRecordUtils.deserializePayload(
            actual.getPayload().getValue(), EntityChangeEvent.class);
    boolean requiredFieldsMatch =
        expectedChangeEvent.getEntityType().equals(actualChangeEvent.getEntityType())
            && expectedChangeEvent.getEntityUrn().equals(actualChangeEvent.getEntityUrn())
            && expectedChangeEvent.getCategory().equals(actualChangeEvent.getCategory())
            && expectedChangeEvent.getOperation().equals(actualChangeEvent.getOperation())
            && expectedChangeEvent.getAuditStamp().equals(actualChangeEvent.getAuditStamp())
            && expectedChangeEvent.getVersion().equals(actualChangeEvent.getVersion());
    boolean modifierMatches =
        !expectedChangeEvent.hasModifier()
            || expectedChangeEvent.getModifier().equals(actualChangeEvent.getModifier());
    boolean parametersMatch =
        !expectedChangeEvent.hasParameters()
            || expectedChangeEvent.getParameters().equals(actualChangeEvent.getParameters());
    return requiredFieldsMatch && modifierMatches && parametersMatch;
  }
}
