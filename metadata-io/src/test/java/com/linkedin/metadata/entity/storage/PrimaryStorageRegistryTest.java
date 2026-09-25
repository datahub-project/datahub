package com.linkedin.metadata.entity.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import io.datahubproject.metadata.context.StorageTarget;
import io.ebean.Database;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class PrimaryStorageRegistryTest {

  @Test
  public void registerAndGet_roundTrip() {
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(true);
    Database database = Mockito.mock(Database.class);
    registry.register(StorageTarget.PRIMARY, database);

    assertTrue(registry.has(StorageTarget.PRIMARY));
    assertEquals(registry.get(StorageTarget.PRIMARY, Database.class), database);
    assertTrue(registry.isDistinctReadEndpoint());
  }

  @Test
  public void getOptional_emptyWhenMissing() {
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(false);
    assertFalse(registry.getOptional(StorageTarget.READ).isPresent());
  }

  @Test
  public void get_throwsWhenUnregistered() {
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(false);
    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class, () -> registry.get(StorageTarget.READ, Database.class));
    assertTrue(ex.getMessage().contains("READ"));
  }

  @Test
  public void get_throwsWhenWrongType() {
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(false);
    registry.register(StorageTarget.PRIMARY, Mockito.mock(Database.class));
    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class, () -> registry.get(StorageTarget.PRIMARY, String.class));
    assertTrue(ex.getMessage().contains("not of type"));
  }
}
