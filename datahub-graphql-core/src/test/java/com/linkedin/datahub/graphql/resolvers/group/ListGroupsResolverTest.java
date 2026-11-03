package com.linkedin.datahub.graphql.resolvers.group;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.CORP_GROUP_CREATED_TIME_INDEX_FIELD_NAME;
import static com.linkedin.metadata.Constants.CORP_GROUP_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListGroupsInput;
import com.linkedin.datahub.graphql.generated.SearchSortInput;
import com.linkedin.datahub.graphql.generated.SortCriterion;
import com.linkedin.datahub.graphql.generated.SortOrder;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListGroupsResolverTest {

  private static final com.linkedin.metadata.query.filter.SortCriterion CREATED_TIME_DESC =
      new com.linkedin.metadata.query.filter.SortCriterion()
          .setField(CORP_GROUP_CREATED_TIME_INDEX_FIELD_NAME)
          .setOrder(com.linkedin.metadata.query.filter.SortOrder.DESCENDING);

  private static final com.linkedin.metadata.query.filter.SortCriterion NAME_ASC =
      new com.linkedin.metadata.query.filter.SortCriterion()
          .setField("name")
          .setOrder(com.linkedin.metadata.query.filter.SortOrder.ASCENDING);

  @Test
  public void testDefaultSortIsNewestFirst() throws Exception {
    assertEquals(sortCriteriaSentToSearch(new ListGroupsInput()), List.of(CREATED_TIME_DESC));
  }

  @Test
  public void testSortCriteriaPrecedeTheDefaultSort() throws Exception {
    SearchSortInput sortInput = new SearchSortInput();
    sortInput.setSortCriteria(List.of(nameAscending()));
    ListGroupsInput input = new ListGroupsInput();
    input.setSortInput(sortInput);

    assertEquals(sortCriteriaSentToSearch(input), List.of(NAME_ASC, CREATED_TIME_DESC));
  }

  @Test
  public void testDeprecatedSingleSortCriterionPrecedesTheDefaultSort() throws Exception {
    // A single sortCriterion maps to an immutable list; the default sort must still be appended.
    SearchSortInput sortInput = new SearchSortInput();
    sortInput.setSortCriterion(nameAscending());
    ListGroupsInput input = new ListGroupsInput();
    input.setSortInput(sortInput);

    assertEquals(sortCriteriaSentToSearch(input), List.of(NAME_ASC, CREATED_TIME_DESC));
  }

  private static SortCriterion nameAscending() {
    SortCriterion sortCriterion = new SortCriterion();
    sortCriterion.setField("name");
    sortCriterion.setSortOrder(SortOrder.ASCENDING);
    return sortCriterion;
  }

  @SuppressWarnings("unchecked")
  private static List<com.linkedin.metadata.query.filter.SortCriterion> sortCriteriaSentToSearch(
      ListGroupsInput input) throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.search(
                any(), eq(CORP_GROUP_ENTITY_NAME), anyString(), any(), any(), anyInt(), anyInt()))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(0)
                .setNumEntities(0)
                .setEntities(new SearchEntityArray()));

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    new ListGroupsResolver(mockClient).get(mockEnv).get();

    ArgumentCaptor<List<com.linkedin.metadata.query.filter.SortCriterion>> sortCaptor =
        ArgumentCaptor.forClass(List.class);
    Mockito.verify(mockClient)
        .search(
            any(),
            eq(CORP_GROUP_ENTITY_NAME),
            anyString(),
            any(),
            sortCaptor.capture(),
            anyInt(),
            anyInt());
    return sortCaptor.getValue();
  }
}
