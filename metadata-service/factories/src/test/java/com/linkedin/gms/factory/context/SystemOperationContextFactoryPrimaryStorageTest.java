package com.linkedin.gms.factory.context;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.entity.storage.PrimaryStorageRegistry;
import com.linkedin.metadata.entity.storage.PrimaryStorageResolver;
import io.datahubproject.metadata.context.PrimaryStorageContext;
import io.datahubproject.metadata.context.ReadPreference;
import io.datahubproject.metadata.context.StorageTarget;
import io.ebean.Database;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

public class SystemOperationContextFactoryPrimaryStorageTest {

  @Test
  public void primaryStorageContext_withoutResolver_returnsEmpty() {
    PrimaryStorageContext context =
        ReflectionTestUtils.invokeMethod(
            SystemOperationContextFactory.class, "primaryStorageContext", (Object) null);

    assertEquals(context, PrimaryStorageContext.EMPTY);
  }

  @Test
  public void primaryStorageContext_withResolver_usesRegistryDefaults() {
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(true);
    registry.register(StorageTarget.PRIMARY, Mockito.mock(Database.class));
    registry.register(StorageTarget.READ, Mockito.mock(Database.class));
    PrimaryStorageResolver resolver = new PrimaryStorageResolver(registry, ReadPreference.READ);

    PrimaryStorageContext context =
        ReflectionTestUtils.invokeMethod(
            SystemOperationContextFactory.class, "primaryStorageContext", resolver);

    assertEquals(context.getReadPreference(), ReadPreference.READ);
    assertEquals(context.isIncludeReadPreferenceInEntityCacheKey(), true);
  }
}
