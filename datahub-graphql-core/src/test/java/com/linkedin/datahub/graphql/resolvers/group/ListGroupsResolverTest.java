package com.linkedin.datahub.graphql.resolvers.group;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.CORP_GROUP_CREATED_TIME_INDEX_FIELD_NAME;
import static com.linkedin.metadata.Constants.CORP_GROUP_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListGroupsInput;
import com.linkedin.datahub.graphql.generated.ListGroupsResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListGroupsResolverTest {

  private static final Urn TEST_GROUP_URN = UrnUtils.getUrn("urn:li:corpGroup:test-group");

  @Test
  public void testBrowseSortsNewestFirst() throws Exception {
    ListGroupsInput input = new ListGroupsInput();
    input.setStart(0);
    input.setCount(25);

    List<SortCriterion> sortCriteria = runAndCaptureSort(input, "");

    assertEquals(sortCriteria.size(), 1);
    assertEquals(sortCriteria.get(0).getField(), CORP_GROUP_CREATED_TIME_INDEX_FIELD_NAME);
    assertEquals(sortCriteria.get(0).getOrder(), SortOrder.DESCENDING);
  }

  @Test
  public void testQueryRanksByRelevance() throws Exception {
    ListGroupsInput input = new ListGroupsInput();
    input.setStart(0);
    input.setCount(25);
    input.setQuery("data-governance-apps");

    List<SortCriterion> sortCriteria = runAndCaptureSort(input, "data-governance-apps");

    assertTrue(
        sortCriteria.stream()
            .noneMatch(c -> CORP_GROUP_CREATED_TIME_INDEX_FIELD_NAME.equals(c.getField())));
  }

  @SuppressWarnings("unchecked")
  private static List<SortCriterion> runAndCaptureSort(ListGroupsInput input, String expectedQuery)
      throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.search(
                any(), anyString(), anyString(), any(), any(List.class), anyInt(), anyInt()))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableList.of(new SearchEntity().setEntity(TEST_GROUP_URN)))));

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    ListGroupsResult result = new ListGroupsResolver(mockClient).get(mockEnv).get();
    assertEquals(result.getGroups().get(0).getUrn(), TEST_GROUP_URN.toString());

    ArgumentCaptor<List<SortCriterion>> sortCaptor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(mockClient)
        .search(
            any(),
            eq(CORP_GROUP_ENTITY_NAME),
            eq(expectedQuery),
            any(),
            sortCaptor.capture(),
            eq(0),
            eq(25));
    return sortCaptor.getValue();
  }
}
