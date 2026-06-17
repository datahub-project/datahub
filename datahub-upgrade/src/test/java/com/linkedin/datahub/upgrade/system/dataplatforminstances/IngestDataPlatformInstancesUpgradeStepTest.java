package com.linkedin.datahub.upgrade.system.dataplatforminstances;

import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestDataPlatformInstancesUpgradeStepTest {

  @Mock private EntityService<?> mockEntityService;
  @Mock private AspectMigrationsDao mockMigrationsDao;
  @Mock private UpgradeContext mockUpgradeContext;
  @Mock private OperationContext mockOpContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockUpgradeContext.opContext()).thenReturn(mockOpContext);
  }

  @Test
  public void testSkipWhenDisabled() {
    IngestDataPlatformInstancesUpgradeStep step =
        new IngestDataPlatformInstancesUpgradeStep(mockEntityService, mockMigrationsDao, false);

    assertTrue(step.skip(mockUpgradeContext));
    verify(mockMigrationsDao, never()).checkIfAspectExists(any(OperationContext.class), any());
  }

  @Test
  public void testSkipWhenAspectExists() {
    when(mockMigrationsDao.checkIfAspectExists(
            any(OperationContext.class), eq(DATA_PLATFORM_INSTANCE_ASPECT_NAME)))
        .thenReturn(true);

    IngestDataPlatformInstancesUpgradeStep step =
        new IngestDataPlatformInstancesUpgradeStep(mockEntityService, mockMigrationsDao, true);

    assertTrue(step.skip(mockUpgradeContext));
  }

  @Test
  public void testNoSkipWhenEnabledAndAspectMissing() {
    when(mockMigrationsDao.checkIfAspectExists(
            any(OperationContext.class), eq(DATA_PLATFORM_INSTANCE_ASPECT_NAME)))
        .thenReturn(false);

    IngestDataPlatformInstancesUpgradeStep step =
        new IngestDataPlatformInstancesUpgradeStep(mockEntityService, mockMigrationsDao, true);

    assertFalse(step.skip(mockUpgradeContext));
  }

  @Test
  public void testExecutableSucceedsWithNoEntities() throws Exception {
    when(mockMigrationsDao.countEntities(any(OperationContext.class))).thenReturn(0L);

    IngestDataPlatformInstancesUpgradeStep step =
        new IngestDataPlatformInstancesUpgradeStep(mockEntityService, mockMigrationsDao, true);

    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    verify(mockEntityService, never()).ingestAspects(any(), any(), anyBoolean(), anyBoolean());
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutableFailsOnException() {
    when(mockMigrationsDao.countEntities(any(OperationContext.class)))
        .thenThrow(new RuntimeException("db error"));

    IngestDataPlatformInstancesUpgradeStep step =
        new IngestDataPlatformInstancesUpgradeStep(mockEntityService, mockMigrationsDao, true);

    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }
}
