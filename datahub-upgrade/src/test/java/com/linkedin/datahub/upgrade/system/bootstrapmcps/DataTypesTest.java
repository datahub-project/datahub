package com.linkedin.datahub.upgrade.system.bootstrapmcps;

import static com.linkedin.datahub.upgrade.system.bootstrapmcps.BootstrapMCPUtilTest.OP_CONTEXT;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeManager;
import com.linkedin.datahub.upgrade.UpgradeResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeManager;
import com.linkedin.datatype.DataTypeInfo;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.List;
import org.testng.annotations.Test;

public class DataTypesTest {

  private static final Urn TEST_DATA_TYPE_URN = UrnUtils.getUrn("urn:li:dataType:datahub.test");

  @Test
  public void testExecuteValidDataTypesNoExistingDataTypes() throws Exception {
    final EntityService<?> entityService = mock(EntityService.class);
    final UpgradeManager upgradeManager =
        loadContext("bootstrapmcp_datatypes/test_valid.yaml", entityService);

    // run the upgrade
    upgradeManager.execute(OP_CONTEXT, "BootstrapMCP", List.of());

    DataTypeInfo expectedResult = new DataTypeInfo();
    expectedResult.setDescription("Test Description");
    expectedResult.setDisplayName("Test Name");
    expectedResult.setQualifiedName("datahub.test");

    verify(entityService, times(1))
        .ingestProposal(any(OperationContext.class), any(AspectsBatchImpl.class), eq(true));
  }

  @Test
  public void testExecuteInvalidJson() throws Exception {
    final EntityService<?> entityService = mock(EntityService.class);
    final UpgradeManager upgradeManager =
        loadContext("bootstrapmcp_datatypes/test_invalid.yaml", entityService);

    UpgradeResult upgradeResult = upgradeManager.execute(OP_CONTEXT, "BootstrapMCP", List.of());

    assertEquals(upgradeResult.result(), DataHubUpgradeState.FAILED);

    // verify expected existence check
    verify(entityService)
        .exists(
            any(OperationContext.class),
            eq(UrnUtils.getUrn("urn:li:dataHubUpgrade:bootstrap-data-types-v1")),
            eq("dataHubUpgradeResult"),
            anyBoolean());

    // Verify no additional interactions
    verifyNoMoreInteractions(entityService);
  }

  private static UpgradeManager loadContext(String configFile, EntityService<?> entityService)
      throws IOException {
    // hasn't run
    when(entityService.exists(
            any(OperationContext.class), any(Urn.class), eq("dataHubUpgradeResult"), anyBoolean()))
        .thenReturn(false);

    Upgrade bootstrapUpgrade = new BootstrapMCP(OP_CONTEXT, configFile, entityService, false);
    assertFalse(bootstrapUpgrade.steps().isEmpty());
    return new DefaultUpgradeManager().register(bootstrapUpgrade);
  }
}
