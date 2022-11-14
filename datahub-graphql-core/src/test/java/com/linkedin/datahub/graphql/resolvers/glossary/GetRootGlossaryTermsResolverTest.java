package com.linkedin.datahub.graphql.resolvers.glossary;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GetRootGlossaryEntitiesInput;
import com.linkedin.datahub.graphql.generated.GetRootGlossaryTermsResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class GetRootGlossaryTermsResolverTest {
  final GetRootGlossaryEntitiesInput testInput = new GetRootGlossaryEntitiesInput(0, 100);
  final String glossaryTermUrn1 = "urn:li:glossaryTerm:11115397daf94708a8822b8106cfd451";
  final String glossaryTermUrn2 = "urn:li:glossaryTerm:22225397daf94708a8822b8106cfd451";


  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryContext mockContext = Mockito.mock(QueryContext.class);

    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(testInput);

    Mockito.when(mockClient.filter(
        Mockito.eq(Constants.GLOSSARY_TERM_ENTITY_NAME),
        Mockito.eq(buildGlossaryEntitiesFilter()),
        Mockito.eq(null),
        Mockito.eq(0),
        Mockito.eq(100),
        Mockito.any(Authentication.class)
    )).thenReturn(
        new SearchResult().setEntities(new SearchEntityArray(ImmutableSet.of(
            new SearchEntity()
                .setEntity(Urn.createFromString(glossaryTermUrn1)),
            new SearchEntity()
                .setEntity(Urn.createFromString(glossaryTermUrn2))
        ))).setFrom(0).setNumEntities(2)
    );

    GetRootGlossaryTermsResolver resolver = new GetRootGlossaryTermsResolver(mockClient);
    GetRootGlossaryTermsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getCount(), 2);
    assertEquals(result.getStart(), 0);
    assertEquals(result.getTotal(), 2);
    assertEquals(result.getTerms().get(0).getUrn(), Urn.createFromString(glossaryTermUrn1).toString());
    assertEquals(result.getTerms().get(1).getUrn(), Urn.createFromString(glossaryTermUrn2).toString());
  }

  private Filter buildGlossaryEntitiesFilter() {
    CriterionArray array = new CriterionArray(
        ImmutableList.of(
            new Criterion()
                .setField("hasParentNode")
                .setValue("false")
                .setCondition(Condition.EQUAL)
        ));
    final Filter filter = new Filter();
    filter.setOr(new ConjunctiveCriterionArray(ImmutableList.of(
        new ConjunctiveCriterion()
            .setAnd(array)
    )));
    return filter;
  }
}
