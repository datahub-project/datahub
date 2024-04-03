package com.linkedin.datahub.graphql.resolvers;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static org.testng.AssertJUnit.assertEquals;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ResolverUtilsTest {

  @Test
  public void testCriterionFromFilter() throws Exception {
    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    final QueryContext mockAllowContext = TestUtils.getMockAllowContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockAllowContext);

    // this is the expected path
    Criterion valuesCriterion =
        criterionFromFilter(
            new FacetFilterInput(
                "tags",
                null,
                ImmutableList.of("urn:li:tag:abc", "urn:li:tag:def"),
                false,
                FilterOperator.EQUAL));
    assertEquals(
        valuesCriterion,
        new Criterion()
            .setValue("urn:li:tag:abc")
            .setValues(new StringArray(ImmutableList.of("urn:li:tag:abc", "urn:li:tag:def")))
            .setNegated(false)
            .setCondition(Condition.EQUAL)
            .setField("tags.keyword"));

    // this is the legacy pathway
    Criterion valueCriterion =
        criterionFromFilter(
            new FacetFilterInput("tags", "urn:li:tag:abc", null, true, FilterOperator.EQUAL));
    assertEquals(
        valueCriterion,
        new Criterion()
            .setValue("urn:li:tag:abc")
            .setValues(new StringArray(ImmutableList.of("urn:li:tag:abc")))
            .setNegated(true)
            .setCondition(Condition.EQUAL)
            .setField("tags.keyword"));

    // check that both being null doesn't cause a NPE. this should never happen except via API
    // interaction
    Criterion doubleNullCriterion =
        criterionFromFilter(new FacetFilterInput("tags", null, null, true, FilterOperator.EQUAL));
    assertEquals(
        doubleNullCriterion,
        new Criterion()
            .setValue("")
            .setValues(new StringArray(ImmutableList.of()))
            .setNegated(true)
            .setCondition(Condition.EQUAL)
            .setField("tags.keyword"));
  }

  @Test
  public void testBuildFilterWithUrns() throws Exception {
    Urn urn1 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz1)");
    Urn urn2 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz2)");
    Set<Urn> urns = new HashSet<>();
    urns.add(urn1);
    urns.add(urn2);

    Criterion ownersCriterion =
        new Criterion()
            .setField("owners")
            .setValues(new StringArray("urn:li:corpuser:chris"))
            .setCondition(Condition.EQUAL);
    CriterionArray andCriterionArray = new CriterionArray(ImmutableList.of(ownersCriterion));
    final Filter filter = new Filter();
    filter.setOr(
        new ConjunctiveCriterionArray(
            ImmutableList.of(new ConjunctiveCriterion().setAnd(andCriterionArray))));

    Filter finalFilter = buildFilterWithUrns(urns, filter);

    Criterion urnsCriterion =
        new Criterion()
            .setField("urn")
            .setValue("")
            .setValues(
                new StringArray(urns.stream().map(Object::toString).collect(Collectors.toList())));

    for (ConjunctiveCriterion conjunctiveCriterion : finalFilter.getOr()) {
      assertEquals(conjunctiveCriterion.getAnd().contains(ownersCriterion), true);
      assertEquals(conjunctiveCriterion.getAnd().contains(urnsCriterion), true);
    }
  }
}
