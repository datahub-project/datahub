package com.linkedin.metadata.test.eval;

import static com.linkedin.metadata.utils.SearchUtil.INDEX_VIRTUAL_FIELD;
import static com.linkedin.metadata.utils.SearchUtil.URN_FIELD;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.virtualFields.VirtualFieldsQueryEvaluator;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

public class VirtualFieldsQueryEvaluatorTest {

  private VirtualFieldsQueryEvaluator virtualFieldsQueryEvaluator =
      new VirtualFieldsQueryEvaluator();

  @Test
  public void testIsEligibleTrueForEntityTypeQuery() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn(INDEX_VIRTUAL_FIELD);
    when(query.getQueryParts()).thenReturn(ImmutableList.of(INDEX_VIRTUAL_FIELD));
    assertTrue(virtualFieldsQueryEvaluator.isEligible("dataset", query));
  }

  @Test
  public void testIsEligibleTrueForUrnQuery() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn(INDEX_VIRTUAL_FIELD);
    when(query.getQueryParts()).thenReturn(ImmutableList.of(URN_FIELD));
    assertTrue(virtualFieldsQueryEvaluator.isEligible("dataset", query));
  }
}
