package com.linkedin.datahub.upgrade.system.dataproducts;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BackfillDataProductAssetsTest {

  private OperationContext opContext;
  private EntityService<?> entityService;
  private SearchService searchService;

  @BeforeMethod
  public void setUp() {
    opContext = TestOperationContexts.systemContextNoValidate();
    entityService = mock(EntityService.class);
    searchService = mock(SearchService.class);
  }

  @Test
  public void testEnabledRegistersStep() {
    BackfillDataProductAssets upgrade =
        new BackfillDataProductAssets(opContext, entityService, searchService, true, 100);

    assertEquals(upgrade.id(), "BackfillDataProductAssets");
    assertEquals(upgrade.steps().size(), 1);
    assertTrue(upgrade.steps().get(0) instanceof BackfillDataProductAssetsStep);
  }

  @Test
  public void testDisabledRegistersNoSteps() {
    BackfillDataProductAssets upgrade =
        new BackfillDataProductAssets(opContext, entityService, searchService, false, 100);

    assertTrue(upgrade.steps().isEmpty());
  }
}
