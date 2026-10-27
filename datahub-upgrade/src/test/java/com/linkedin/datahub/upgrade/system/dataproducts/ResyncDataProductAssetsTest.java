package com.linkedin.datahub.upgrade.system.dataproducts;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ResyncDataProductAssetsTest {

  private OperationContext opContext;
  private EntityService<?> entityService;
  private AspectDao aspectDao;

  @BeforeMethod
  public void setUp() {
    opContext = TestOperationContexts.systemContextNoValidate();
    entityService = mock(EntityService.class);
    aspectDao = mock(AspectDao.class);
  }

  @Test
  public void testEnabledRegistersStep() {
    ResyncDataProductAssets upgrade =
        new ResyncDataProductAssets(opContext, entityService, aspectDao, true, 100, 0, 0, false);

    assertEquals(upgrade.id(), "ResyncDataProductAssets");
    assertEquals(upgrade.steps().size(), 1);
    assertTrue(upgrade.steps().get(0) instanceof ResyncDataProductAssetsStep);
  }

  @Test
  public void testDisabledRegistersNoSteps() {
    ResyncDataProductAssets upgrade =
        new ResyncDataProductAssets(opContext, entityService, aspectDao, false, 100, 0, 0, true);

    assertTrue(upgrade.steps().isEmpty());
  }
}
