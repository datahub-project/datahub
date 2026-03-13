package com.linkedin.datahub.upgrade;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.function.Function;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PersistentUpgradeStepTest {

  private EntityService<?> mockEntityService;
  private OperationContext mockOpContext;
  private Urn testUrn;

  @BeforeMethod
  public void setup() throws Exception {
    mockEntityService = mock(EntityService.class);
    mockOpContext = mock(OperationContext.class);
    testUrn = Urn.createFromString("urn:li:dataHubUpgrade:TestStep");
  }

  @Test
  public void testSkipWhenPreviouslyRun() {
    // Setup: Step was previously run
    when(mockEntityService.exists(
            eq(mockOpContext), eq(testUrn), eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME), eq(true)))
        .thenReturn(true);

    TestPersistentStep step =
        new TestPersistentStep(mockOpContext, mockEntityService, testUrn, false);
    UpgradeContext context = mock(UpgradeContext.class);

    // Execute
    boolean shouldSkip = step.skip(context);

    // Assert
    assertTrue(shouldSkip, "Should skip when previously run");
  }

  @Test
  public void testDoNotSkipWhenNotPreviouslyRun() {
    // Setup: Step was NOT previously run
    when(mockEntityService.exists(
            eq(mockOpContext), eq(testUrn), eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME), eq(true)))
        .thenReturn(false);

    TestPersistentStep step =
        new TestPersistentStep(mockOpContext, mockEntityService, testUrn, false);
    UpgradeContext context = mock(UpgradeContext.class);

    // Execute
    boolean shouldSkip = step.skip(context);

    // Assert
    assertFalse(shouldSkip, "Should NOT skip when not previously run");
  }

  @Test
  public void testReprocessEnabled() {
    // Setup: Reprocess is enabled (should always run)
    when(mockEntityService.exists(any(), any(), any(), anyBoolean())).thenReturn(true);

    TestPersistentStep step =
        new TestPersistentStep(mockOpContext, mockEntityService, testUrn, true);
    UpgradeContext context = mock(UpgradeContext.class);

    // Execute
    boolean shouldSkip = step.skip(context);

    // Assert
    assertFalse(shouldSkip, "Should NOT skip when reprocess is enabled, even if previously run");
  }

  // Test implementation
  private static class TestPersistentStep implements PersistentUpgradeStep {
    private final OperationContext opContext;
    private final EntityService<?> entityService;
    private final Urn upgradeUrn;
    private final boolean reprocessEnabled;

    public TestPersistentStep(
        OperationContext opContext,
        EntityService<?> entityService,
        Urn upgradeUrn,
        boolean reprocessEnabled) {
      this.opContext = opContext;
      this.entityService = entityService;
      this.upgradeUrn = upgradeUrn;
      this.reprocessEnabled = reprocessEnabled;
    }

    @Override
    public Urn getUpgradeIdUrn() {
      return upgradeUrn;
    }

    @Override
    public EntityService<?> getEntityService() {
      return entityService;
    }

    @Override
    public OperationContext getSystemOpContext() {
      return opContext;
    }

    @Override
    public boolean isReprocessEnabled() {
      return reprocessEnabled;
    }

    @Override
    public String id() {
      return "TestStep";
    }

    @Override
    public Function<UpgradeContext, UpgradeStepResult> executable() {
      return (context) -> new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    }
  }
}
