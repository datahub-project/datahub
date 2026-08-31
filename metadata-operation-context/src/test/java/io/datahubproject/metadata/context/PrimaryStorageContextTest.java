package io.datahubproject.metadata.context;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class PrimaryStorageContextTest {

  @Test
  public void empty_usesPrimaryWithoutCacheKey() {
    assertEquals(PrimaryStorageContext.EMPTY.getReadPreference(), ReadPreference.PRIMARY);
    assertFalse(PrimaryStorageContext.EMPTY.getCacheKeyComponent().isPresent());
    assertFalse(PrimaryStorageContext.EMPTY.isIncludeReadPreferenceInEntityCacheKey());
  }

  @Test
  public void withReadPreference_updatesPreference() {
    PrimaryStorageContext context =
        PrimaryStorageContext.EMPTY.withReadPreference(ReadPreference.READ);
    assertEquals(context.getReadPreference(), ReadPreference.READ);
  }

  @Test
  public void getCacheKeyComponent_includesOverrideWhenDistinctReplica() {
    PrimaryStorageContext context =
        PrimaryStorageContext.builder()
            .readPreference(ReadPreference.READ)
            .storageTargetOverride(StorageTarget.READ)
            .includeReadPreferenceInEntityCacheKey(true)
            .build();

    assertTrue(context.getCacheKeyComponent().isPresent());
    assertEquals(context.getStorageTargetOverride().get(), StorageTarget.READ);
  }
}
