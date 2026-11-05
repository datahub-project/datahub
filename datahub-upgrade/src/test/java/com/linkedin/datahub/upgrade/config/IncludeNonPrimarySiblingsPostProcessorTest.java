package com.linkedin.datahub.upgrade.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.Test;

public class IncludeNonPrimarySiblingsPostProcessorTest {

  private final IncludeNonPrimarySiblingsPostProcessor processor =
      new IncludeNonPrimarySiblingsPostProcessor();

  @Test
  public void testUpgradeSystemContextSeesEveryMember() {
    OperationContext system = TestOperationContexts.systemContextNoSearchAuthorization();
    Object processed = processor.postProcessAfterInitialization(system, "systemOperationContext");
    assertTrue(
        ((OperationContext) processed)
            .getSearchContext()
            .getSearchFlags()
            .isIncludeNonPrimarySiblings());
    // The bean it was built from keeps its own flags.
    assertNotEquals(
        system.getSearchContext().getSearchFlags().isIncludeNonPrimarySiblings(), Boolean.TRUE);
  }

  @Test
  public void testOtherBeansAreUntouched() {
    OperationContext other = TestOperationContexts.systemContextNoSearchAuthorization();
    assertSame(processor.postProcessAfterInitialization(other, "someOtherContext"), other);
    Object notAContext = "value";
    assertEquals(
        processor.postProcessAfterInitialization(notAContext, "systemOperationContext"),
        notAContext);
  }
}
