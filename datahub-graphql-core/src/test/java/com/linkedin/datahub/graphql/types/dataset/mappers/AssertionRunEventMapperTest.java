package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.types.common.mappers.util.UnrepresentableValueException;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlingsForTests;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AssertionRunEventMapperTest {
  @Test
  public void testValuesFromNewerVersion() {
    // Read after a rollback: a result whose required type only a newer version defines is left
    // out, and a newer status fails only this event (the run event list leaves it out).
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);
    try {
      AssertionRunEvent newerResultType = baseRunEvent();
      newerResultType.setResult(new AssertionResult().setType(AssertionResultType.SUCCESS));
      newerResultType.getResult().data().put("type", "RESULT_TYPE_FROM_NEWER_BUILD");
      Assert.assertNull(map(newerResultType).getResult());

      AssertionRunEvent newerStatus = baseRunEvent();
      newerStatus.data().put("status", "STATUS_FROM_NEWER_BUILD");
      Assert.assertThrows(UnrepresentableValueException.class, () -> map(newerStatus));
    } finally {
      UnknownDataHandlingsForTests.reset();
    }
  }

  @Test
  public void testValuesFromNewerVersionStrictHandlingFails() {
    UnknownDataHandlingsForTests.use(StrictHandling.INSTANCE);
    try {
      AssertionRunEvent newerResultType = baseRunEvent();
      newerResultType.setResult(new AssertionResult().setType(AssertionResultType.SUCCESS));
      newerResultType.getResult().data().put("type", "RESULT_TYPE_FROM_NEWER_BUILD");
      Assert.assertThrows(IllegalArgumentException.class, () -> map(newerResultType));
    } finally {
      UnknownDataHandlingsForTests.reset();
    }
  }

  private static com.linkedin.datahub.graphql.generated.AssertionRunEvent map(
      AssertionRunEvent input) {
    return AssertionRunEventMapper.map(
        null, new EnvelopedAspect().setAspect(GenericRecordUtils.serializeAspect(input)));
  }

  private AssertionRunEvent baseRunEvent() {
    final AssertionRunEvent runEvent = new AssertionRunEvent();
    runEvent.setTimestampMillis(1000L);
    runEvent.setRunId("test-run-id");
    runEvent.setStatus(AssertionRunStatus.COMPLETE);
    runEvent.setAssertionUrn(UrnUtils.getUrn("urn:li:assertion:test"));
    runEvent.setAsserteeUrn(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.t,PROD)"));
    return runEvent;
  }
}
