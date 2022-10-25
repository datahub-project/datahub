package com.linkedin.datahub.graphql.resolvers;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import graphql.schema.DataFetchingEnvironment;
import junit.framework.TestCase;
import org.testng.annotations.Test;
import org.mockito.Mockito;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


public class ResolverUtilsTest extends TestCase {

  @Test
  public void testCriterionFromFilter() throws Exception {
    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    final QueryContext mockAllowContext = TestUtils.getMockAllowContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockAllowContext);

    // this is the expected path
    Criterion valuesCriterion = criterionFromFilter(
        new FacetFilterInput(
            "tags",
            null,
            ImmutableList.of("urn:li:tag:abc", "urn:li:tag:def"),
            false,
            FilterOperator.EQUAL
        )
    );
    assertEquals(valuesCriterion, new Criterion().setValue("urn:li:tag:abc").setValues(
        new StringArray(ImmutableList.of("urn:li:tag:abc", "urn:li:tag:def"))
    ).setNegated(false).setCondition(Condition.EQUAL).setField("tags.keyword"));

    // this is the legacy pathway
    Criterion valueCriterion = criterionFromFilter(
        new FacetFilterInput(
            "tags",
            "urn:li:tag:abc",
            null,
            true,
            FilterOperator.EQUAL
        )
    );
    assertEquals(valueCriterion, new Criterion().setValue("urn:li:tag:abc").setValues(
        new StringArray(ImmutableList.of("urn:li:tag:abc"))
    ).setNegated(true).setCondition(Condition.EQUAL).setField("tags.keyword"));

    // check that both being null doesn't cause a NPE. this should never happen except via API interaction
    Criterion doubleNullCriterion = criterionFromFilter(
        new FacetFilterInput(
            "tags",
            null,
            null,
            true,
            FilterOperator.EQUAL
        )
    );
    assertEquals(doubleNullCriterion, new Criterion().setValue("").setValues(
        new StringArray(ImmutableList.of())
    ).setNegated(true).setCondition(Condition.EQUAL).setField("tags.keyword"));
  }
}
