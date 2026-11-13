package com.linkedin.metadata.aspect.validators;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.aspect.validation.ConditionalWriteValidator;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.metadata.aspect.batch.TestSystemAspect;
import org.testng.annotations.Test;

public class ConditionalWriteValidatorVersionRuleTest {

  @Test
  public void usesSystemMetadataVersionWhenPresent() {
    TestSystemAspect latestRow =
        TestSystemAspect.builder()
            .version(0)
            .systemMetadata(new SystemMetadata().setVersion("7"))
            .build();

    assertEquals(ConditionalWriteValidator.resolveAspectVersion(latestRow), 7L);
  }

  @Test
  public void fallsBackToRowVersionFlooredAtOne() {
    TestSystemAspect unversionedLatestRow =
        TestSystemAspect.builder().version(0).systemMetadata(new SystemMetadata()).build();
    TestSystemAspect unversionedHistoryRow = TestSystemAspect.builder().version(4).build();

    assertEquals(ConditionalWriteValidator.resolveAspectVersion(unversionedLatestRow), 1L);
    assertEquals(ConditionalWriteValidator.resolveAspectVersion(unversionedHistoryRow), 4L);
  }
}
