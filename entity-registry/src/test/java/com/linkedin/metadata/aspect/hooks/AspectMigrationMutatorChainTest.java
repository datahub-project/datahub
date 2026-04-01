package com.linkedin.metadata.aspect.hooks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.util.Pair;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for {@link AspectMigrationMutatorChain}. */
public class AspectMigrationMutatorChainTest {

  private static final String OWNERSHIP_ASPECT_NAME = "ownership";
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

  private EntityRegistry entityRegistry;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setUp() {
    entityRegistry = new TestEntityRegistry();
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    retrieverContext = mock(RetrieverContext.class);
    when(retrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
  }

  // ── Empty chain ────────────────────────────────────────────────────────────

  @Test
  public void testEmptyChain_selfDisables() {
    AspectMigrationMutatorChain chain = chain(Collections.emptyList());
    assertFalse(chain.isEnabled(), "Empty chain should self-disable on construction");
  }

  @Test
  public void testEmptyChain_writeMutation_passesThrough() {
    AspectMigrationMutatorChain chain = chain(Collections.emptyList());

    ChangeMCP item = ownershipItem();
    List<Pair<ChangeMCP, Boolean>> results =
        chain.writeMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertEquals(results.size(), 1);
    assertFalse(results.get(0).getSecond(), "Disabled chain should be a no-op");
  }

  @Test
  public void testEmptyChain_readMutation_passesThrough() {
    AspectMigrationMutatorChain chain = chain(Collections.emptyList());

    ChangeMCP item = ownershipItem();
    List<Pair<ReadItem, Boolean>> results =
        chain.readMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertEquals(results.size(), 1);
    assertFalse(results.get(0).getSecond(), "Disabled chain should be a no-op on read path");
  }

  // ── Single-hop chain ───────────────────────────────────────────────────────

  @Test
  public void testSingleHop_writeMutation_appliesMigration() {
    TestOwnershipMutator mutator = mutator(1L, 2L);
    AspectMigrationMutatorChain chain = chain(List.of(mutator));

    ChangeMCP item = ownershipItem(); // no schemaVersion → DEFAULT_SCHEMA_VERSION (1)

    List<Pair<ChangeMCP, Boolean>> results =
        chain.writeMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertEquals(results.size(), 1);
    assertTrue(results.get(0).getSecond(), "Expected mutation=true");
    assertEquals((long) item.getSystemMetadata().getSchemaVersion(), 2L);
  }

  @Test
  public void testSingleHop_writeMutation_alreadyMigrated_passesThrough() {
    TestOwnershipMutator mutator = mutator(1L, 2L);
    AspectMigrationMutatorChain chain = chain(List.of(mutator));

    ChangeMCP item = ownershipItem(2L); // already at v2

    List<Pair<ChangeMCP, Boolean>> results =
        chain.writeMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertEquals(results.size(), 1);
    assertFalse(results.get(0).getSecond(), "Expected no mutation for already-migrated item");
    assertEquals((long) item.getSystemMetadata().getSchemaVersion(), 2L); // unchanged
  }

  @Test
  public void testSingleHop_readMutation_appliesMigration() {
    TestOwnershipMutator mutator = mutator(1L, 2L);
    AspectMigrationMutatorChain chain = chain(List.of(mutator));

    ChangeMCP item = ownershipItem(); // no schemaVersion → DEFAULT_SCHEMA_VERSION (1)

    List<Pair<ReadItem, Boolean>> results =
        chain.readMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertEquals(results.size(), 1);
    assertTrue(results.get(0).getSecond(), "Expected mutation=true on read path");
    assertEquals((long) item.getSystemMetadata().getSchemaVersion(), 2L);
  }

  @Test
  public void testSingleHop_readMutation_alreadyMigrated_passesThrough() {
    TestOwnershipMutator mutator = mutator(1L, 2L);
    AspectMigrationMutatorChain chain = chain(List.of(mutator));

    ChangeMCP item = ownershipItem(2L); // already at v2

    List<Pair<ReadItem, Boolean>> results =
        chain.readMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertEquals(results.size(), 1);
    assertFalse(results.get(0).getSecond(), "Expected no mutation for already-migrated item");
    assertEquals((long) item.getSystemMetadata().getSchemaVersion(), 2L); // unchanged
  }

  // ── Multi-hop chain ────────────────────────────────────────────────────────

  @Test
  public void testMultiHop_writeMutation_appliesBothHops() {
    // v1→v2 then v2→v3
    TestOwnershipMutator hop1 = mutator(1L, 2L);
    TestOwnershipMutator hop2 = mutator(2L, 3L);
    AspectMigrationMutatorChain chain = chain(List.of(hop1, hop2));

    ChangeMCP item =
        ownershipItem(); // no schemaVersion → DEFAULT_SCHEMA_VERSION (1), both hops fire

    List<Pair<ChangeMCP, Boolean>> results =
        chain.writeMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertEquals(results.size(), 1);
    assertTrue(results.get(0).getSecond(), "Expected mutation=true");
    assertEquals((long) item.getSystemMetadata().getSchemaVersion(), 3L);
  }

  // ── Self-disable ───────────────────────────────────────────────────────────

  @Test
  public void testSelfDisable_afterDisable() {
    TestOwnershipMutator mutator = mutator(1L, 2L);
    AspectMigrationMutatorChain chain = chain(List.of(mutator));
    assertTrue(chain.isEnabled(), "Chain should start enabled");

    chain.disable();
    assertFalse(chain.isEnabled(), "Chain should be disabled after disable()");
  }

  @Test
  public void testSelfDisable_disabledChain_isNoOp() {
    TestOwnershipMutator mutator = mutator(1L, 2L);
    AspectMigrationMutatorChain chain = chain(List.of(mutator));
    chain.disable();

    ChangeMCP item = ownershipItem();
    List<Pair<ChangeMCP, Boolean>> results =
        chain.writeMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertFalse(results.get(0).getSecond(), "Disabled chain should not apply mutations");
    assertFalse(
        item.getSystemMetadata().hasSchemaVersion(),
        "Disabled chain should not update schema version");
  }

  // ── Priority ───────────────────────────────────────────────────────────────

  @Test
  public void testChain_hasHighestPriority() {
    AspectMigrationMutatorChain chain = chain(Collections.emptyList());
    assertEquals(chain.getPriority(), MutationHook.MIGRATION_PRIORITY);
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private AspectMigrationMutatorChain chain(List<AspectMigrationMutator> mutators) {
    AspectMigrationMutatorChain c = new AspectMigrationMutatorChain(mutators);
    c.setConfig(
        AspectPluginConfig.builder()
            .className(AspectMigrationMutatorChain.class.getName())
            .enabled(true)
            .supportedOperations(List.of("*"))
            .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
            .build());
    return c;
  }

  /**
   * Item with no schemaVersion set — treated as {@link
   * AspectMigrationMutator#DEFAULT_SCHEMA_VERSION}.
   */
  private ChangeMCP ownershipItem() {
    return ownershipItem(null);
  }

  private ChangeMCP ownershipItem(long schemaVersion) {
    return ownershipItem((Long) schemaVersion);
  }

  private ChangeMCP ownershipItem(@Nullable Long schemaVersion) {
    Ownership ownership = new Ownership();
    ownership.setOwners(new OwnerArray());
    SystemMetadata sm = new SystemMetadata();
    if (schemaVersion != null) {
      sm.setSchemaVersion(schemaVersion);
    }
    return TestMCP.builder()
        .urn(DATASET_URN)
        .changeType(ChangeType.UPSERT)
        .entitySpec(entityRegistry.getEntitySpec("dataset"))
        .aspectSpec(entityRegistry.getAspectSpecs().get(OWNERSHIP_ASPECT_NAME))
        .recordTemplate(ownership)
        .systemMetadata(sm)
        .build();
  }

  static class TestOwnershipMutator extends AspectMigrationMutator {

    private final long sourceVersion;
    private final long targetVersion;

    TestOwnershipMutator() {
      this(1L, 2L);
    }

    TestOwnershipMutator(long sourceVersion, long targetVersion) {
      this.sourceVersion = sourceVersion;
      this.targetVersion = targetVersion;
    }

    @Override
    public String getAspectName() {
      return OWNERSHIP_ASPECT_NAME;
    }

    @Override
    public long getSourceVersion() {
      return sourceVersion;
    }

    @Override
    public long getTargetVersion() {
      return targetVersion;
    }

    @Override
    @Nullable
    protected RecordTemplate transform(
        @Nonnull RecordTemplate sourceAspect, @Nonnull RetrieverContext context) {
      Ownership migrated = new Ownership();
      migrated.setOwners(new OwnerArray());
      return migrated;
    }
  }

  private TestOwnershipMutator mutator(long sourceVersion, long targetVersion) {
    return new TestOwnershipMutator(sourceVersion, targetVersion);
  }
}
