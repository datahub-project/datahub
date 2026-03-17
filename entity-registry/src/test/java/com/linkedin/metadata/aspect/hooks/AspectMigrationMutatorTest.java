package com.linkedin.metadata.aspect.hooks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
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

/**
 * Unit tests for {@link AspectMigrationMutator}.
 *
 * <p>Uses a trivial concrete mutator that marks the Ownership aspect as "migrated" by setting a
 * well-known owner count on the DataMap, so we can assert the transform was applied.
 */
public class AspectMigrationMutatorTest {

  private static final String OWNERSHIP_ASPECT_NAME = "ownership";
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

  private EntityRegistry entityRegistry;
  private RetrieverContext retrieverContext;

  /** Minimal mutator: v1 → v2, touches the Ownership aspect. */
  static class TestOwnershipMutator extends AspectMigrationMutator {

    @Override
    public String getAspectName() {
      return OWNERSHIP_ASPECT_NAME;
    }

    @Override
    public long getSourceVersion() {
      return 1L;
    }

    @Override
    public long getTargetVersion() {
      return 2L;
    }

    /** Returns a new Ownership with an empty owner list — distinct from the input. */
    @Override
    @Nullable
    protected RecordTemplate transform(
        @Nonnull RecordTemplate sourceAspect, @Nonnull RetrieverContext context) {
      Ownership migrated = new Ownership();
      migrated.setOwners(new OwnerArray());
      return migrated;
    }
  }

  @BeforeMethod
  public void setUp() {
    entityRegistry = new TestEntityRegistry();
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    retrieverContext = mock(RetrieverContext.class);
    when(retrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
  }

  // ── Read mutation ──────────────────────────────────────────────────────────

  @Test
  public void testReadMutation_atSourceVersion_transforms() {
    TestOwnershipMutator mutator = mutator();

    Ownership original = new Ownership();
    original.setOwners(new OwnerArray()); // non-null to confirm transform replaces content
    SystemMetadata sm = new SystemMetadata();
    // No schemaVersion set → treated as DEFAULT_SCHEMA_VERSION (1)

    TestMCP item =
        TestMCP.builder()
            .urn(DATASET_URN)
            .changeType(ChangeType.UPSERT)
            .entitySpec(entityRegistry.getEntitySpec("dataset"))
            .aspectSpec(entityRegistry.getAspectSpecs().get(OWNERSHIP_ASPECT_NAME))
            .recordTemplate(original)
            .systemMetadata(sm)
            .build();

    List<Pair<com.linkedin.metadata.aspect.ReadItem, Boolean>> results =
        mutator
            .readMutation(Collections.singletonList(item), retrieverContext)
            .collect(Collectors.toList());

    assertEquals(results.size(), 1);
    assertTrue(results.get(0).getSecond(), "Expected mutation=true");
    // DataMap was mutated in-place; schemaVersion updated to 2
    assertEquals((long) item.getSystemMetadata().getSchemaVersion(), 2L);
  }

  @Test
  public void testReadMutation_atTargetVersion_passesThrough() {
    TestOwnershipMutator mutator = mutator();

    Ownership original = new Ownership();
    original.setOwners(new OwnerArray());
    SystemMetadata sm = new SystemMetadata();
    sm.setSchemaVersion(2L); // already at target version → no-op

    TestMCP item =
        TestMCP.builder()
            .urn(DATASET_URN)
            .changeType(ChangeType.UPSERT)
            .entitySpec(entityRegistry.getEntitySpec("dataset"))
            .aspectSpec(entityRegistry.getAspectSpecs().get(OWNERSHIP_ASPECT_NAME))
            .recordTemplate(original)
            .systemMetadata(sm)
            .build();

    List<Pair<com.linkedin.metadata.aspect.ReadItem, Boolean>> results =
        mutator
            .readMutation(Collections.singletonList(item), retrieverContext)
            .collect(Collectors.toList());

    assertEquals(results.size(), 1);
    assertFalse(results.get(0).getSecond(), "Expected mutation=false for already-migrated aspect");
    assertEquals((long) item.getSystemMetadata().getSchemaVersion(), 2L); // unchanged
  }

  @Test
  public void testReadMutation_differentAspect_passesThrough() {
    TestOwnershipMutator mutator = mutator();

    TestMCP item =
        TestMCP.builder()
            .urn(DATASET_URN)
            .changeType(ChangeType.UPSERT)
            .entitySpec(entityRegistry.getEntitySpec("dataset"))
            .aspectSpec(entityRegistry.getAspectSpecs().get("schemaMetadata"))
            .recordTemplate(new com.linkedin.schema.SchemaMetadata())
            .systemMetadata(new SystemMetadata())
            .build();

    List<Pair<com.linkedin.metadata.aspect.ReadItem, Boolean>> results =
        mutator
            .readMutation(Collections.singletonList(item), retrieverContext)
            .collect(Collectors.toList());

    assertEquals(results.size(), 1);
    assertFalse(results.get(0).getSecond(), "Expected mutation=false for different aspect");
  }

  // ── Write mutation ─────────────────────────────────────────────────────────

  @Test
  public void testWriteMutation_atSourceVersion_transformsAndBumpsVersion() {
    TestOwnershipMutator mutator = mutator();

    Ownership original = new Ownership();
    original.setOwners(new OwnerArray());
    SystemMetadata sm = new SystemMetadata();
    // No schemaVersion → treated as DEFAULT_SCHEMA_VERSION (1)

    ChangeMCP item =
        TestMCP.builder()
            .urn(DATASET_URN)
            .changeType(ChangeType.UPSERT)
            .entitySpec(entityRegistry.getEntitySpec("dataset"))
            .aspectSpec(entityRegistry.getAspectSpecs().get(OWNERSHIP_ASPECT_NAME))
            .recordTemplate(original)
            .systemMetadata(sm)
            .build();

    List<Pair<ChangeMCP, Boolean>> results =
        mutator
            .writeMutation(Collections.singletonList(item), retrieverContext)
            .collect(Collectors.toList());

    assertEquals(results.size(), 1);
    assertTrue(results.get(0).getSecond(), "Expected mutation=true");
    assertNotNull(item.getSystemMetadata());
    assertEquals((long) item.getSystemMetadata().getSchemaVersion(), 2L);
  }

  @Test
  public void testWriteMutation_atTargetVersion_passesThrough() {
    TestOwnershipMutator mutator = mutator();

    Ownership original = new Ownership();
    original.setOwners(new OwnerArray());
    SystemMetadata sm = new SystemMetadata();
    sm.setSchemaVersion(2L);

    ChangeMCP item =
        TestMCP.builder()
            .urn(DATASET_URN)
            .changeType(ChangeType.UPSERT)
            .entitySpec(entityRegistry.getEntitySpec("dataset"))
            .aspectSpec(entityRegistry.getAspectSpecs().get(OWNERSHIP_ASPECT_NAME))
            .recordTemplate(original)
            .systemMetadata(sm)
            .build();

    List<Pair<ChangeMCP, Boolean>> results =
        mutator
            .writeMutation(Collections.singletonList(item), retrieverContext)
            .collect(Collectors.toList());

    assertEquals(results.size(), 1);
    assertFalse(results.get(0).getSecond(), "Expected mutation=false for already-migrated aspect");
    assertEquals((long) item.getSystemMetadata().getSchemaVersion(), 2L); // unchanged
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private TestOwnershipMutator mutator() {
    return new TestOwnershipMutator();
  }
}
