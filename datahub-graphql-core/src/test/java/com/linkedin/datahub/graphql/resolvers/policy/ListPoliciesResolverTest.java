package com.linkedin.datahub.graphql.resolvers.policy;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.ListPoliciesInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListPoliciesResolverTest {

  private static final String GROUP_URN = "urn:li:corpGroup:group-a";

  @Test
  public void testOrFiltersAreAppliedAsOrOfAndConditions() throws Exception {
    final Filter filter =
        captureFilter(
            List.of(andFilter(facet("groups", GROUP_URN)), andFilter(facet("allGroups", "true"))));

    assertEquals(
        conjunctionFields(filter), List.of(List.of("groups"), List.of("allGroups")), "branches");
  }

  @Test
  public void testSingleOrFilterKeepsAllConditionsInOneConjunction() throws Exception {
    final Filter filter =
        captureFilter(List.of(andFilter(facet("state", "ACTIVE"), facet("editable", "true"))));

    assertEquals(conjunctionFields(filter), List.of(List.of("state", "editable")));
  }

  private static Filter captureFilter(final List<AndFilterInput> orFilters) throws Exception {
    final EntityClient entityClient = Mockito.mock(EntityClient.class);
    final ScrollResult emptyResult = new ScrollResult();
    emptyResult.setEntities(new SearchEntityArray());
    emptyResult.setNumEntities(0);
    emptyResult.setPageSize(0);
    Mockito.when(
            entityClient.scrollAcrossEntities(
                any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(emptyResult);

    final ListPoliciesInput input = new ListPoliciesInput();
    input.setStart(0);
    input.setCount(20);
    input.setOrFilters(orFilters);
    final QueryContext context = getMockAllowContext();
    final DataFetchingEnvironment environment = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(environment.getArgument("input")).thenReturn(input);
    Mockito.when(environment.getContext()).thenReturn(context);

    new ListPoliciesResolver(entityClient).get(environment).get();

    final ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    Mockito.verify(entityClient)
        .scrollAcrossEntities(
            any(), any(), any(), filterCaptor.capture(), any(), any(), any(), any());
    return filterCaptor.getValue();
  }

  private static List<List<String>> conjunctionFields(final Filter filter) {
    return filter.getOr().stream()
        .map(ConjunctiveCriterion::getAnd)
        .map(and -> and.stream().map(Criterion::getField).collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

  private static AndFilterInput andFilter(final FacetFilterInput... facets) {
    final AndFilterInput andFilter = new AndFilterInput();
    andFilter.setAnd(List.of(facets));
    return andFilter;
  }

  private static FacetFilterInput facet(final String field, final String value) {
    final FacetFilterInput facet = new FacetFilterInput();
    facet.setField(field);
    facet.setValues(List.of(value));
    return facet;
  }
}
