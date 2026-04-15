package com.linkedin.datahub.upgrade.system.migrations;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.aspect.hooks.AspectMigrationMutator;
import com.linkedin.metadata.aspect.hooks.AspectMigrationMutatorChain;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MigrateAspectsTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private EntityService<?> mockEntityService;
  private AspectDao mockAspectDao;

  @BeforeMethod
  public void setup() {
    mockEntityService = mock(EntityService.class);
    mockAspectDao = mock(AspectDao.class);
  }

  private static final String UPGRADE_VERSION = "v0.14.1-abc123def456abc123def456abc123def456abc1";

  @Test
  public void testDisabledFlagProducesNoSteps() {
    AspectMigrationMutator mutator = mutatorFor("testAspect", 1L, 2L);
    AspectMigrationMutatorChain chain = new AspectMigrationMutatorChain(List.of(mutator));

    MigrateAspects upgrade =
        new MigrateAspects(
            OP_CONTEXT, mockEntityService, mockAspectDao, chain, UPGRADE_VERSION, false, 100, 0, 0);

    assertTrue(upgrade.steps().isEmpty());
  }

  @Test
  public void testEmptyChainProducesNoSteps() {
    AspectMigrationMutatorChain emptyChain = new AspectMigrationMutatorChain(List.of());

    MigrateAspects upgrade =
        new MigrateAspects(
            OP_CONTEXT,
            mockEntityService,
            mockAspectDao,
            emptyChain,
            UPGRADE_VERSION,
            true,
            100,
            0,
            0);

    assertTrue(upgrade.steps().isEmpty());
  }

  @Test
  public void testEnabledChainWithMutatorsProducesTwoSteps() {
    AspectMigrationMutator mutator = mutatorFor("testAspect", 1L, 2L);
    AspectMigrationMutatorChain chain = new AspectMigrationMutatorChain(List.of(mutator));

    MigrateAspects upgrade =
        new MigrateAspects(
            OP_CONTEXT, mockEntityService, mockAspectDao, chain, UPGRADE_VERSION, true, 100, 0, 0);

    assertEquals(upgrade.steps().size(), 1);
    assertNotNull(upgrade.steps().get(0));
  }

  @Test
  public void testIdIsFullyQualifiedClassName() {
    AspectMigrationMutatorChain chain = new AspectMigrationMutatorChain(List.of());

    MigrateAspects upgrade =
        new MigrateAspects(
            OP_CONTEXT, mockEntityService, mockAspectDao, chain, UPGRADE_VERSION, false, 100, 0, 0);

    assertEquals(upgrade.id(), MigrateAspects.class.getName());
  }

  @Test
  public void testStepIdEncodesUpgradeVersion() {
    AspectMigrationMutator mutatorV2 = mutatorFor("aspectA", 1L, 2L);
    AspectMigrationMutatorChain chain = new AspectMigrationMutatorChain(List.of(mutatorV2));

    MigrateAspects upgrade =
        new MigrateAspects(
            OP_CONTEXT, mockEntityService, mockAspectDao, chain, UPGRADE_VERSION, true, 100, 0, 0);

    // Migration step ID should encode the software version, not an aspect schema version
    assertEquals(upgrade.steps().get(0).id(), "migrate-aspects-" + UPGRADE_VERSION);
  }

  // ── helpers ────────────────────────────────────────────────────────────────

  private static AspectMigrationMutator mutatorFor(
      String aspectName, long sourceVersion, long targetVersion) {
    AspectMigrationMutator m = mock(AspectMigrationMutator.class);
    when(m.getAspectName()).thenReturn(aspectName);
    when(m.getSourceVersion()).thenReturn(sourceVersion);
    when(m.getTargetVersion()).thenReturn(targetVersion);
    return m;
  }
}
