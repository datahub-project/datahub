package com.linkedin.datahub.graphql.resolvers.container;

import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.ContainerEntitiesInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ContainerEntitiesResolverTest {

  private static final ContainerEntitiesInput TEST_INPUT =
      new ContainerEntitiesInput(null, 0, 20, Collections.emptyList());

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    final String childUrn = "urn:li:dataset:(test,test,test)";
    final String containerUrn = "urn:li:container:test-container";

    final Criterion filterCriterion =
        new Criterion()
            .setField("container.keyword")
            .setCondition(Condition.EQUAL)
            .setValue(containerUrn);

    Mockito.when(
            mockClient.searchAcrossEntities(
                Mockito.eq(ContainerEntitiesResolver.CONTAINABLE_ENTITY_NAMES),
                Mockito.eq("*"),
                Mockito.eq(
                    new Filter()
                        .setOr(
                            new ConjunctiveCriterionArray(
                                new ConjunctiveCriterion()
                                    .setAnd(
                                        new CriterionArray(ImmutableList.of(filterCriterion)))))),
                Mockito.eq(0),
                Mockito.eq(20),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.any(Authentication.class)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(
                            new SearchEntity().setEntity(Urn.createFromString(childUrn)))))
                .setMetadata(
                    new SearchResultMetadata().setAggregations(new AggregationMetadataArray())));

    ContainerEntitiesResolver resolver = new ContainerEntitiesResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Container parentContainer = new Container();
    parentContainer.setUrn(containerUrn);
    Mockito.when(mockEnv.getSource()).thenReturn(parentContainer);

    // Data Assertions
    assertEquals((int) resolver.get(mockEnv).get().getStart(), 0);
    assertEquals((int) resolver.get(mockEnv).get().getCount(), 1);
    assertEquals((int) resolver.get(mockEnv).get().getTotal(), 1);
    assertEquals(resolver.get(mockEnv).get().getSearchResults().size(), 1);
    assertEquals(
        resolver.get(mockEnv).get().getSearchResults().get(0).getEntity().getUrn(), childUrn);
  }
}
