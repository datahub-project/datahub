package com.linkedin.metadata.aspect.consistency;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.linkedin.metadata.aspect.consistency.fix.BatchItemsFix;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFix;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixDetail;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.aspect.consistency.fix.HardDeleteEntityFix;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for ConsistencyFixRegistry. */
public class ConsistencyFixRegistryTest {

  private ConsistencyFixRegistry registry;
  private BatchItemsFix batchItemsFix;
  private HardDeleteEntityFix hardDeleteFix;

  /** Custom fix for testing */
  private static class TestCustomFix implements ConsistencyFix {
    @Override
    @Nonnull
    public ConsistencyFixType getType() {
      return ConsistencyFixType.TRIM_UPSERT;
    }

    @Override
    @Nonnull
    public ConsistencyFixDetail apply(
        @Nonnull OperationContext opContext, @Nonnull ConsistencyIssue issue, boolean dryRun) {
      return ConsistencyFixDetail.builder()
          .urn(issue.getEntityUrn())
          .action(ConsistencyFixType.TRIM_UPSERT)
          .success(true)
          .build();
    }
  }

  @BeforeMethod
  public void setup() {
    EntityService<?> mockEntityService = mock(EntityService.class);

    batchItemsFix = new BatchItemsFix(mockEntityService);
    hardDeleteFix = new HardDeleteEntityFix(mockEntityService);

    registry =
        new ConsistencyFixRegistry(List.of(batchItemsFix, hardDeleteFix, new TestCustomFix()));
  }

  @Test
  public void testGetFixReturnsCorrectFix() {
    // BatchItemsFix handles CREATE, UPSERT, PATCH, SOFT_DELETE, DELETE_ASPECT
    Optional<ConsistencyFix> createFix = registry.getFix(ConsistencyFixType.CREATE);
    assertTrue(createFix.isPresent());
    assertSame(createFix.get(), batchItemsFix);

    Optional<ConsistencyFix> upsertFix = registry.getFix(ConsistencyFixType.UPSERT);
    assertTrue(upsertFix.isPresent());
    assertSame(upsertFix.get(), batchItemsFix);

    Optional<ConsistencyFix> softDeleteFix = registry.getFix(ConsistencyFixType.SOFT_DELETE);
    assertTrue(softDeleteFix.isPresent());
    assertSame(softDeleteFix.get(), batchItemsFix);

    // HardDeleteEntityFix handles HARD_DELETE
    Optional<ConsistencyFix> hardDelete = registry.getFix(ConsistencyFixType.HARD_DELETE);
    assertTrue(hardDelete.isPresent());
    assertSame(hardDelete.get(), hardDeleteFix);

    // TestCustomFix handles TRIM_UPSERT
    Optional<ConsistencyFix> trimFix = registry.getFix(ConsistencyFixType.TRIM_UPSERT);
    assertTrue(trimFix.isPresent());
    assertEquals(trimFix.get().getType(), ConsistencyFixType.TRIM_UPSERT);
  }

  @Test
  public void testGetFixReturnsEmptyForUnregisteredType() {
    Optional<ConsistencyFix> fix = registry.getFix(ConsistencyFixType.DELETE_INDEX_DOCUMENTS);
    assertFalse(fix.isPresent());
  }

  @Test
  public void testGetFixOrThrowReturnsFixForRegisteredType() {
    ConsistencyFix fix = registry.getFixOrThrow(ConsistencyFixType.HARD_DELETE);
    assertNotNull(fix);
    assertSame(fix, hardDeleteFix);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetFixOrThrowThrowsForUnregisteredType() {
    registry.getFixOrThrow(ConsistencyFixType.DELETE_INDEX_DOCUMENTS);
  }

  @Test
  public void testHasFixReturnsTrue() {
    assertTrue(registry.hasFix(ConsistencyFixType.CREATE));
    assertTrue(registry.hasFix(ConsistencyFixType.UPSERT));
    assertTrue(registry.hasFix(ConsistencyFixType.PATCH));
    assertTrue(registry.hasFix(ConsistencyFixType.SOFT_DELETE));
    assertTrue(registry.hasFix(ConsistencyFixType.DELETE_ASPECT));
    assertTrue(registry.hasFix(ConsistencyFixType.HARD_DELETE));
    assertTrue(registry.hasFix(ConsistencyFixType.TRIM_UPSERT));
  }

  @Test
  public void testHasFixReturnsFalse() {
    assertFalse(registry.hasFix(ConsistencyFixType.DELETE_INDEX_DOCUMENTS));
  }

  @Test
  public void testBatchItemsFixRegistersMultipleTypes() {
    // Verify all BatchItemsFix.SUPPORTED_TYPES are registered
    for (ConsistencyFixType type : BatchItemsFix.SUPPORTED_TYPES) {
      assertTrue(registry.hasFix(type), "BatchItemsFix should be registered for " + type);
      assertSame(
          registry.getFix(type).orElse(null), batchItemsFix, "BatchItemsFix should handle " + type);
    }
  }

  @Test
  public void testSize() {
    // BatchItemsFix registers for 5 types (CREATE, UPSERT, PATCH, SOFT_DELETE, DELETE_ASPECT)
    // HardDeleteEntityFix registers for 1 type (HARD_DELETE)
    // TestCustomFix registers for 1 type (TRIM_UPSERT)
    // Total: 7 entries
    assertEquals(registry.size(), 7);
  }

  @Test
  public void testEmptyRegistry() {
    ConsistencyFixRegistry emptyRegistry = new ConsistencyFixRegistry(List.of());
    assertEquals(emptyRegistry.size(), 0);
    assertFalse(emptyRegistry.hasFix(ConsistencyFixType.HARD_DELETE));
    assertFalse(emptyRegistry.getFix(ConsistencyFixType.HARD_DELETE).isPresent());
  }
}
