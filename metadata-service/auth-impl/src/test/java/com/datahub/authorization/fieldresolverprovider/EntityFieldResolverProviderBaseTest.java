package com.datahub.authorization.fieldresolverprovider;

import static org.testng.Assert.assertEquals;

import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;

public abstract class EntityFieldResolverProviderBaseTest<T extends EntityFieldResolverProvider> {
  protected abstract T buildFieldResolverProvider();

  @Test
  public void testEmpty() throws ExecutionException, InterruptedException {
    assertEquals(
        buildFieldResolverProvider()
            .getFieldResolver(new EntitySpec("dataset", ""))
            .getFieldValuesFuture()
            .get(),
        FieldResolver.getResolverFromValues(Collections.emptySet()).getFieldValuesFuture().get());
  }
}
