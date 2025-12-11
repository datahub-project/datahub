/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
