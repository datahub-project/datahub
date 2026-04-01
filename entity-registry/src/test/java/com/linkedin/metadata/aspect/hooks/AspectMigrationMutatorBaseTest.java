package com.linkedin.metadata.aspect.hooks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Base test class for {@link AspectMigrationMutator} implementations. Enforces the version contract
 * and tests the happy path for both write and read mutations, so concrete mutator tests can focus
 * on supplying realistic aspect payloads and asserting the correctness of the transformed result.
 *
 * <p>Extend this class for every concrete {@link AspectMigrationMutator} implementation. All
 * contract-level {@code @Test} methods are inherited automatically — the subclass only needs to
 * supply three things:
 *
 * <ol>
 *   <li>{@link #mutator()} — the implementation under test
 *   <li>{@link #provideSourceAspect()} — a realistic aspect payload at {@code sourceVersion}
 *   <li>{@link #assertTransformed(RecordTemplate)} — assertions specific to the migration logic
 * </ol>
 *
 * <p>Example subclass:
 *
 * <pre>{@code
 * public class MyAspectV1ToV2MigratorTest extends AspectMigrationMutatorBaseTest {
 *
 *   @Override protected AspectMigrationMutator mutator() {
 *     return new MyAspectV1ToV2Migrator();
 *   }
 *
 *   @Override protected RecordTemplate provideSourceAspect() {
 *     Owner v1 = new Owner();
 *     v1.setOldField("someValue");
 *     return v1;
 *   }
 *
 *   @Override protected void assertTransformed(RecordTemplate result) {
 *     Owner migrated = (Owner) result;
 *     assertEquals(migrated.getNewField(), "expected value");
 *     assertFalse(migrated.hasOldField());
 *   }
 * }
 * }</pre>
 */
public abstract class AspectMigrationMutatorBaseTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

  // Abstract

  /** Returns the {@link AspectMigrationMutator} implementation under test. */
  @Nonnull
  protected abstract AspectMigrationMutator mutator();

  /**
   * Returns a realistic aspect payload that is at {@link AspectMigrationMutator#getSourceVersion()}
   * and ready to be transformed by the mutator.
   */
  @Nonnull
  protected abstract RecordTemplate provideSourceAspect();

  /**
   * Asserts that the transformed aspect payload is correct. Called after a successful write or read
   * mutation at {@code sourceVersion}.
   *
   * @param result the aspect payload after {@code transform()} has been applied (read from the item
   *     via {@link ChangeMCP#getRecordTemplate()})
   */
  protected abstract void assertTransformed(@Nonnull RecordTemplate result);

  // Shared

  protected EntityRegistry entityRegistry;
  protected RetrieverContext retrieverContext;

  @BeforeMethod
  public void setUp() {
    entityRegistry = new TestEntityRegistry();
    AspectRetriever mockRetriever = mock(AspectRetriever.class);
    when(mockRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    retrieverContext = mock(RetrieverContext.class);
    when(retrieverContext.getAspectRetriever()).thenReturn(mockRetriever);
  }

  // Version contract (auto-inherited)

  @Test
  public void contract_targetVersionIsSourceVersionPlusOne() {
    assertEquals(
        mutator().getTargetVersion(),
        mutator().getSourceVersion() + 1,
        "targetVersion must equal sourceVersion + 1");
  }

  @Test
  public void contract_sourceVersionAtLeastDefault() {
    assertTrue(
        mutator().getSourceVersion() >= AspectMigrationMutator.DEFAULT_SCHEMA_VERSION,
        "sourceVersion must be >= DEFAULT_SCHEMA_VERSION");
  }

  // Write path (auto-inherited)

  @Test
  public void writeMutation_atSourceVersion_mutatesAndBumpsVersion() {
    SystemMetadata sm = new SystemMetadata();
    sm.setSchemaVersion(mutator().getSourceVersion());

    ChangeMCP item = buildItem(provideSourceAspect(), sm);

    List<Pair<ChangeMCP, Boolean>> results =
        mutator().writeMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertTrue(results.get(0).getSecond(), "Expected mutated=true at sourceVersion");
    assertEquals(
        (long) item.getSystemMetadata().getSchemaVersion(),
        mutator().getTargetVersion(),
        "schemaVersion must be bumped to targetVersion");

    assertTransformed(item.getRecordTemplate());
  }

  @Test
  public void writeMutation_atDefaultSchemaVersion_mutatesAndBumpsVersion() {
    // null schemaVersion is treated as DEFAULT_SCHEMA_VERSION — only relevant when sourceVersion==1
    if (mutator().getSourceVersion() != AspectMigrationMutator.DEFAULT_SCHEMA_VERSION) {
      return;
    }
    ChangeMCP item = buildItem(provideSourceAspect(), new SystemMetadata()); // no schemaVersion set

    List<Pair<ChangeMCP, Boolean>> results =
        mutator().writeMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertTrue(results.get(0).getSecond(), "Expected mutated=true for null schemaVersion (=1)");
    assertEquals((long) item.getSystemMetadata().getSchemaVersion(), mutator().getTargetVersion());
  }

  @Test
  public void writeMutation_alreadyAtTargetVersion_isNoOp() {
    SystemMetadata sm = new SystemMetadata();
    sm.setSchemaVersion(mutator().getTargetVersion());

    ChangeMCP item = buildItem(provideSourceAspect(), sm);

    List<Pair<ChangeMCP, Boolean>> results =
        mutator().writeMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertFalse(results.get(0).getSecond(), "Expected no-op when already at targetVersion");
    assertEquals(
        (long) item.getSystemMetadata().getSchemaVersion(),
        mutator().getTargetVersion(),
        "schemaVersion must remain unchanged");
  }

  @Test
  public void writeMutation_futureVersion_isNoOp() {
    SystemMetadata sm = new SystemMetadata();
    sm.setSchemaVersion(mutator().getTargetVersion() + 1); // ahead of this mutator

    ChangeMCP item = buildItem(provideSourceAspect(), sm);

    List<Pair<ChangeMCP, Boolean>> results =
        mutator().writeMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertFalse(results.get(0).getSecond(), "Expected no-op for version ahead of targetVersion");
  }

  // Read path (auto-inherited)
  @Test
  public void readMutation_atSourceVersion_mutatesAndBumpsVersion() {
    SystemMetadata sm = new SystemMetadata();
    sm.setSchemaVersion(mutator().getSourceVersion());

    ChangeMCP item = buildItem(provideSourceAspect(), sm);

    List<Pair<ReadItem, Boolean>> results =
        mutator().readMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertTrue(results.get(0).getSecond(), "Expected mutated=true on read path at sourceVersion");
    assertEquals(
        (long) item.getSystemMetadata().getSchemaVersion(),
        mutator().getTargetVersion(),
        "schemaVersion must be bumped on read path");

    assertTransformed(item.getRecordTemplate());
  }

  @Test
  public void readMutation_alreadyAtTargetVersion_isNoOp() {
    SystemMetadata sm = new SystemMetadata();
    sm.setSchemaVersion(mutator().getTargetVersion());

    ChangeMCP item = buildItem(provideSourceAspect(), sm);

    List<Pair<ReadItem, Boolean>> results =
        mutator().readMutation(List.of(item), retrieverContext).collect(Collectors.toList());

    assertFalse(results.get(0).getSecond(), "Expected no-op on read path when already migrated");
  }

  // Helper
  protected ChangeMCP buildItem(RecordTemplate record, SystemMetadata sm) {
    return TestMCP.builder()
        .urn(DATASET_URN)
        .changeType(ChangeType.UPSERT)
        .entitySpec(entityRegistry.getEntitySpec("dataset"))
        .aspectSpec(entityRegistry.getAspectSpecs().get(mutator().getAspectName()))
        .recordTemplate(record)
        .systemMetadata(sm)
        .build();
  }
}
