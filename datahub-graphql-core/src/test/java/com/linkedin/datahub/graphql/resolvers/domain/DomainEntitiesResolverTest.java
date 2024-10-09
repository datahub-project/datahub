package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.DomainEntitiesInput;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
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
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DomainEntitiesResolverTest {

  private static final DomainEntitiesInput TEST_INPUT =
      new DomainEntitiesInput(null, 0, 20, Collections.emptyList());

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    final String childUrn = "urn:li:dataset:(test,test,test)";
    final String domainUrn = "urn:li:domain:test-domain";

    final Criterion filterCriterion = buildCriterion("domains.keyword", Condition.EQUAL, domainUrn);

    Mockito.when(
            mockClient.searchAcrossEntities(
                any(),
                Mockito.eq(
                    SEARCHABLE_ENTITY_TYPES.stream()
                        .map(EntityTypeMapper::getName)
                        .collect(Collectors.toList())),
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
                Mockito.eq(Collections.emptyList()),
                Mockito.eq(null)))
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

    DomainEntitiesResolver resolver = new DomainEntitiesResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Domain parentDomain = new Domain();
    parentDomain.setUrn(domainUrn);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDomain);

    // Data Assertions
    assertEquals((int) resolver.get(mockEnv).get().getStart(), 0);
    assertEquals((int) resolver.get(mockEnv).get().getCount(), 1);
    assertEquals((int) resolver.get(mockEnv).get().getTotal(), 1);
    assertEquals(resolver.get(mockEnv).get().getSearchResults().size(), 1);
    assertEquals(
        resolver.get(mockEnv).get().getSearchResults().get(0).getEntity().getUrn(), childUrn);
  }
}
