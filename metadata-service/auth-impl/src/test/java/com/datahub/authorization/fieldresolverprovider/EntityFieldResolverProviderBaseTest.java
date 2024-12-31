package com.datahub.authorization.fieldresolverprovider;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;

public abstract class EntityFieldResolverProviderBaseTest<T extends EntityFieldResolverProvider> {
  protected abstract T buildFieldResolverProvider();

  @Test
  public void testEmpty() throws ExecutionException, InterruptedException {
    assertEquals(
        buildFieldResolverProvider()
            .getFieldResolver(mock(OperationContext.class), new EntitySpec("dataset", ""))
            .getFieldValuesFuture()
            .get(),
        FieldResolver.getResolverFromValues(Collections.emptySet()).getFieldValuesFuture().get());
  }
}
