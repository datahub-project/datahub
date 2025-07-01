package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GetRootGlossaryEntitiesInput;
import com.linkedin.datahub.graphql.generated.GetRootGlossaryNodesResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class GetRootGlossaryNodesResolverTest {
  final GetRootGlossaryEntitiesInput testInput = new GetRootGlossaryEntitiesInput(0, 100);
  final String glossaryNodeUrn1 = "urn:li:glossaryNode:11115397daf94708a8822b8106cfd451";
  final String glossaryNodeUrn2 = "urn:li:glossaryNode:22225397daf94708a8822b8106cfd451";

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(testInput);

    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq(Constants.GLOSSARY_NODE_ENTITY_NAME),
                Mockito.eq(buildGlossaryEntitiesFilter()),
                Mockito.eq(null),
                Mockito.eq(0),
                Mockito.eq(100)))
        .thenReturn(
            new SearchResult()
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(
                            new SearchEntity().setEntity(Urn.createFromString(glossaryNodeUrn1)),
                            new SearchEntity().setEntity(Urn.createFromString(glossaryNodeUrn2)))))
                .setFrom(0)
                .setNumEntities(2));

    GetRootGlossaryNodesResolver resolver = new GetRootGlossaryNodesResolver(mockClient);
    GetRootGlossaryNodesResult result = resolver.get(mockEnv).get();

    assertEquals(result.getCount(), 2);
    assertEquals(result.getStart(), 0);
    assertEquals(result.getTotal(), 2);
    assertEquals(
        result.getNodes().get(0).getUrn(), Urn.createFromString(glossaryNodeUrn1).toString());
    assertEquals(
        result.getNodes().get(1).getUrn(), Urn.createFromString(glossaryNodeUrn2).toString());
  }

  private Filter buildGlossaryEntitiesFilter() {
    CriterionArray array =
        new CriterionArray(
            ImmutableList.of(buildCriterion("hasParentNode", Condition.EQUAL, "false")));
    final Filter filter = new Filter();
    filter.setOr(
        new ConjunctiveCriterionArray(ImmutableList.of(new ConjunctiveCriterion().setAnd(array))));
    return filter;
  }
}
