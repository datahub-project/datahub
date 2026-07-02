package com.linkedin.datahub.graphql.types.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.entity.EntityService;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class EntityExistsTypeTest {

  private static final Urn EXISTING_URN = UrnUtils.getUrn("urn:li:corpuser:exists");
  private static final Urn MISSING_URN = UrnUtils.getUrn("urn:li:corpuser:missing");

  @Test
  public void testBatchLoadCoalescesAndPreservesOrder() throws Exception {
    EntityService<?> entityService = mock(EntityService.class);
    QueryContext context = mock(QueryContext.class);
    when(context.getOperationContext()).thenReturn(mock(OperationContext.class));
    when(entityService.exists(any(OperationContext.class), any(Collection.class)))
        .thenReturn(Set.of(EXISTING_URN));

    EntityExistsType type = new EntityExistsType(entityService);
    List<DataFetcherResult<Boolean>> results =
        type.batchLoad(List.of(EXISTING_URN.toString(), MISSING_URN.toString()), context);

    // One existence check is issued for the whole batch, results keep the input order.
    verify(entityService, times(1)).exists(any(OperationContext.class), any(Collection.class));
    assertEquals(results.get(0).getData(), Boolean.TRUE);
    assertEquals(results.get(1).getData(), Boolean.FALSE);
  }

  @Test
  public void testName() {
    assertEquals(new EntityExistsType(mock(EntityService.class)).name(), "EntityExistsType");
  }
}
